package clickhouse

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"

	chclient "github.com/PromClick/PromClick/clickhouse"
	"github.com/PromClick/PromClick/types"
)

// SchemaInfo holds schema metadata for the tagset schema.
type SchemaInfo struct {
	Database        string
	SamplesTable    string // e.g. __ts_samples__float64
	TimeSeriesTable string // e.g. __ts_by_name
	FingerprintCol  string // fingerprint
	TimestampCol    string // poll_epoch_ns
	ValueCol        string // value
	MetricNameCol   string // __name__
	// TimestampNanos: true when the timestamp column stores nanoseconds.
	// FetchSeriesData will convert ms params → ns for queries, and ns results → ms.
	TimestampNanos bool
}

// Pool wraps a ch-go connection pool for native TCP queries.
type Pool struct {
	pool         *chpool.Pool
	Schema       SchemaInfo
	HTTPClient   *http.Client
	httpAddr     string
	httpUser     string
	httpPassword string
}

// NewPool creates a native TCP connection pool to ClickHouse.
func NewPool(addr, database, user, password, httpAddr string, schema SchemaInfo) (*Pool, error) {
	opts := chpool.Options{
		ClientOptions: ch.Options{
			Address:          addr,
			Database:         database,
			User:             user,
			Password:         password,
			Compression:      ch.CompressionLZ4,
			DialTimeout:      5 * time.Second,
			HandshakeTimeout: 5 * time.Second,
		},
		MaxConnLifetime: 30 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
	}
	p, err := chpool.New(context.Background(), opts)
	if err != nil {
		return nil, fmt.Errorf("chpool.New: %w", err)
	}

	pool := &Pool{
		pool:   p,
		Schema: schema,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        20,
				MaxIdleConnsPerHost: 10,
				MaxConnsPerHost:     20,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: 5 * time.Minute,
		},
		httpAddr:     httpAddr,
		httpUser:     user,
		httpPassword: password,
	}

	slog.Info("schema detection", "table", schema.SamplesTable)
	return pool, nil
}

// LabelMatcher for Go-side label filtering (used after SQL lookup).
type LabelMatcher struct {
	Name  string
	Op    string // "=", "!=", "=~", "!~"
	Value string
}

// regexCache avoids recompiling per call.
var regexCache sync.Map

func cachedRegexp(pattern string) (*regexp.Regexp, error) {
	if v, ok := regexCache.Load(pattern); ok {
		return v.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile("^(?:" + pattern + ")$")
	if err != nil {
		return nil, err
	}
	regexCache.Store(pattern, re)
	return re, nil
}

func matchAll(labels map[string]string, matchers []LabelMatcher) bool {
	if labels == nil {
		return false
	}
	for _, m := range matchers {
		v := labels[m.Name]
		switch m.Op {
		case "=":
			if v != m.Value {
				return false
			}
		case "!=":
			if v == m.Value {
				return false
			}
		case "=~":
			re, err := cachedRegexp(m.Value)
			if err != nil || !re.MatchString(v) {
				return false
			}
		case "!~":
			re, err := cachedRegexp(m.Value)
			if err != nil || re.MatchString(v) {
				return false
			}
		}
	}
	return true
}

// FetchSeriesData satisfies eval.DataFetcher.
// Looks up fingerprints via __ts_by_name, then fetches samples from __ts_samples__float64.
func (p *Pool) FetchSeriesData(ctx context.Context, sql string, params *chclient.QueryParams) (map[string]*chclient.SeriesData, error) {
	metricName := extractParam(params, "metricName")
	dataStart := extractParam(params, "dataStart")
	dataEnd := extractParam(params, "dataEnd")

	if metricName != "" && dataStart != "" && dataEnd != "" {
		matchers := extractMatchers(params)
		fps, labelMap, err := p.lookupFingerprints(ctx, metricName, matchers)
		if err != nil {
			slog.Warn("fingerprint lookup failed", "error", err)
			return make(map[string]*chclient.SeriesData), nil
		}
		return p.fetchWithFingerprints(ctx, metricName, dataStart, dataEnd, fps, labelMap)
	}

	// No params — fall back to raw SQL (JOIN path not adapted for new schema)
	return p.fetchWithJoin(ctx, sql, params)
}

// lookupFingerprints finds fingerprints matching metricName and matchers using
// the two-path strategy from the Ruby reader:
//
//   - Name-only (no equality tag matchers): query __ts_by_name FINAL directly.
//   - Tag-filtered: query __ts_by_tag FINAL grouped by fingerprint with
//     hasAll(groupUniqArray(concat(tag,'=',value)), [...]) then join __ts_by_name
//     to fetch full label arrays.
//
// Regex (=~, !~) and negative (!=) matchers are always applied in Go after fetching.
//
// Returns:
//   - fps: hex-encoded fingerprints (32 chars each)
//   - labelMap: hex-fp → map[string]string (all tags including __name__)
func (p *Pool) lookupFingerprints(ctx context.Context, metricName string, matchers []LabelMatcher) ([]string, map[string]map[string]string, error) {
	s := p.Schema
	escapedMetric := strings.ReplaceAll(metricName, "'", "''")

	// Partition matchers: equality tag matchers pushed to SQL, rest applied in Go.
	var sqlTagPairs []string // "'key=value'" literals for hasAll()
	var goMatchers []LabelMatcher
	for _, m := range matchers {
		if m.Name == "__name__" || m.Name == s.MetricNameCol {
			continue // already handled by __name__ = 'x' filter
		}
		if m.Op == "=" {
			ek := strings.ReplaceAll(m.Name, "'", "''")
			ev := strings.ReplaceAll(m.Value, "'", "''")
			sqlTagPairs = append(sqlTagPairs, fmt.Sprintf("'%s=%s'", ek, ev))
		} else {
			goMatchers = append(goMatchers, m)
		}
	}

	var q strings.Builder
	fmt.Fprintf(&q, "SELECT hex(fingerprint) AS fp, tag_names, tag_values\n")
	fmt.Fprintf(&q, "FROM %s.%s FINAL\n", s.Database, s.TimeSeriesTable)

	if len(sqlTagPairs) > 0 {
		// Tag-filtered path: subquery __ts_by_tag with hasAll(groupUniqArray(...))
		fmt.Fprintf(&q, "WHERE fingerprint IN (\n")
		fmt.Fprintf(&q, "  SELECT fingerprint\n")
		fmt.Fprintf(&q, "  FROM %s.__ts_by_tag FINAL\n", s.Database)
		fmt.Fprintf(&q, "  WHERE __name__ = '%s'\n", escapedMetric)
		fmt.Fprintf(&q, "  GROUP BY fingerprint\n")
		fmt.Fprintf(&q, "  HAVING hasAll(groupUniqArray(concat(tag, '=', value)), [%s])\n",
			strings.Join(sqlTagPairs, ", "))
		fmt.Fprintf(&q, ")\n")
	} else {
		// Name-only path: filter directly by __name__ in __ts_by_name
		fmt.Fprintf(&q, "WHERE %s = '%s'\n", s.MetricNameCol, escapedMetric)
	}

	// Execute query via HTTP JSONEachRow
	u := strings.TrimRight(p.httpAddr, "/") + "/?database=" + s.Database + "&default_format=JSONEachRow"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(q.String()))
	if err != nil {
		return nil, nil, err
	}
	if p.httpUser != "" {
		req.SetBasicAuth(p.httpUser, p.httpPassword)
	}
	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("lookupFingerprints HTTP: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, nil, fmt.Errorf("lookupFingerprints HTTP %d: %s", resp.StatusCode, string(body))
	}

	type fpRow struct {
		FP        string   `json:"fp"`
		TagNames  []string `json:"tag_names"`
		TagValues []string `json:"tag_values"`
	}

	var fps []string
	labelMap := make(map[string]map[string]string)

	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var r fpRow
		if err := dec.Decode(&r); err != nil {
			slog.Warn("lookupFingerprints: skip malformed row", "error", err)
			continue
		}

		// Build label map from tag arrays
		lm := make(map[string]string, len(r.TagNames))
		for i := range r.TagNames {
			if i < len(r.TagValues) {
				lm[r.TagNames[i]] = r.TagValues[i]
			}
		}

		// Apply Go-side matchers (regex, !=, !~)
		if len(goMatchers) > 0 && !matchAll(lm, goMatchers) {
			continue
		}

		fps = append(fps, r.FP)
		labelMap[r.FP] = lm
	}

	return fps, labelMap, nil
}

// fetchWithFingerprints fetches samples from __ts_samples__float64 for the given
// fingerprints and attaches labels from labelMap.
func (p *Pool) fetchWithFingerprints(ctx context.Context,
	metricName, dataStart, dataEnd string,
	fps []string,
	labelMap map[string]map[string]string,
) (map[string]*chclient.SeriesData, error) {
	if len(fps) == 0 {
		return make(map[string]*chclient.SeriesData), nil
	}

	const parallelThreshold = 500
	const numWorkers = 4

	if len(fps) < parallelThreshold {
		return p.fetchChunk(ctx, metricName, dataStart, dataEnd, fps, labelMap)
	}

	chunkSize := (len(fps) + numWorkers - 1) / numWorkers
	type chunkResult struct {
		data map[string]*chclient.SeriesData
		err  error
	}
	results := make([]chunkResult, numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		lo := w * chunkSize
		hi := lo + chunkSize
		if lo >= len(fps) {
			break
		}
		if hi > len(fps) {
			hi = len(fps)
		}
		wg.Add(1)
		go func(idx int, chunk []string) {
			defer wg.Done()
			data, err := p.fetchChunk(ctx, metricName, dataStart, dataEnd, chunk, labelMap)
			results[idx] = chunkResult{data: data, err: err}
		}(w, fps[lo:hi])
	}
	wg.Wait()

	merged := make(map[string]*chclient.SeriesData, len(fps))
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for k, v := range r.data {
			merged[k] = v
		}
	}
	return merged, nil
}

// fetchChunk fetches samples for a subset of fingerprints from __ts_samples__float64.
// Timestamps in the table are nanoseconds; results are converted to milliseconds.
func (p *Pool) fetchChunk(ctx context.Context,
	metricName, dataStart, dataEnd string,
	fps []string,
	labelMap map[string]map[string]string,
) (map[string]*chclient.SeriesData, error) {
	if len(fps) == 0 {
		return make(map[string]*chclient.SeriesData), nil
	}

	s := p.Schema
	escapedMetric := strings.ReplaceAll(metricName, "'", "''")

	// dataStart / dataEnd are milliseconds from the query params.
	// The samples table stores nanoseconds, so multiply by 1,000,000.
	tsStart := dataStart + " * 1000000"
	tsEnd := dataEnd + " * 1000000"
	if !s.TimestampNanos {
		tsStart = dataStart
		tsEnd = dataEnd
	}

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT hex(s.%s) AS fingerprint, s.%s AS ts, s.%s AS value\n",
		s.FingerprintCol, s.TimestampCol, s.ValueCol)
	fmt.Fprintf(&b, "FROM %s.%s AS s\n", s.Database, s.SamplesTable)
	fmt.Fprintf(&b, "PREWHERE s.%s = '%s'\n", s.MetricNameCol, escapedMetric)
	fmt.Fprintf(&b, "WHERE s.%s > %s\n", s.TimestampCol, tsStart)
	fmt.Fprintf(&b, "  AND s.%s <= %s\n", s.TimestampCol, tsEnd)

	if inClause := buildFingerprintIN(fps, fmt.Sprintf("s.%s", s.FingerprintCol)); inClause != "" {
		fmt.Fprintf(&b, "  %s\n", inClause)
	}
	fmt.Fprintf(&b, "ORDER BY s.%s ASC, s.%s ASC", s.FingerprintCol, s.TimestampCol)

	result := make(map[string]*chclient.SeriesData, len(fps))

	var (
		colFP  proto.ColStr
		colTS  proto.ColInt64
		colVal proto.ColFloat64
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: b.String(),
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "ts", Data: &colTS},
			{Name: "value", Data: &colVal},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				fpStr := colFP.Row(i)
				tsVal := colTS.Row(i)

				// Convert nanoseconds → milliseconds for the Series timestamp
				if s.TimestampNanos {
					tsVal /= 1_000_000
				}

				sd, ok := result[fpStr]
				if !ok {
					labels := labelMap[fpStr]
					if labels == nil {
						labels = map[string]string{}
					}
					sd = &chclient.SeriesData{Labels: labels}
					result[fpStr] = sd
				}
				sd.Samples = append(sd.Samples, types.Sample{Timestamp: tsVal, Value: colVal.Row(i)})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("native (cached): %w", err)
	}
	slog.Debug("ch_fetch",
		"path", "lookup+prewhere",
		"metric", metricName,
		"fps", len(fps),
		"series", len(result),
	)
	return result, nil
}

// fetchWithJoin — fallback path using translator-generated SQL.
// NOTE: This path is not adapted for the tagset schema and is retained only
// as a fallback for queries that arrive without full params.
func (p *Pool) fetchWithJoin(ctx context.Context, sql string, params *chclient.QueryParams) (map[string]*chclient.SeriesData, error) {
	t0 := time.Now()
	totalRows := 0
	resolvedSQL := inlineParams(sql, params)
	result := make(map[string]*chclient.SeriesData)

	var (
		colFP     proto.ColStr
		colTS     proto.ColInt64
		colVal    proto.ColFloat64
		colLabels proto.ColStr
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: resolvedSQL,
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "ts", Data: &colTS},
			{Name: "value", Data: &colVal},
			{Name: "labels", Data: &colLabels},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			totalRows += block.Rows
			for i := 0; i < block.Rows; i++ {
				fp := colFP.Row(i)
				ts := colTS.Row(i)
				val := colVal.Row(i)

				sd, ok := result[fp]
				if !ok {
					var lblMap map[string]string
					if err := json.Unmarshal([]byte(colLabels.Row(i)), &lblMap); err != nil {
						lblMap = map[string]string{}
					}
					sd = &chclient.SeriesData{Labels: lblMap}
					result[fp] = sd
				}
				sd.Samples = append(sd.Samples, types.Sample{Timestamp: ts, Value: val})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("native (join): %w", err)
	}
	slog.Debug("ch_fetch",
		"path", "join",
		"rows", totalRows,
		"series", len(result),
		"duration", time.Since(t0),
	)
	return result, nil
}

// ExecTierQuery executes a downsampled query and returns Matrix result.
func (p *Pool) ExecTierQuery(ctx context.Context, sql string, fps []string) (types.Matrix, error) {
	t0 := time.Now()
	totalRows := 0
	seriesMap := make(map[string]*types.Series)

	var (
		colFP  proto.ColStr
		colTS  proto.ColDateTime
		colVal proto.ColFloat64
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: sql,
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "step_ts", Data: &colTS},
			{Name: "value", Data: &colVal},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			totalRows += block.Rows
			for i := 0; i < block.Rows; i++ {
				fp := colFP.Row(i)
				ts := colTS.Row(i).UnixMilli()

				s, ok := seriesMap[fp]
				if !ok {
					fpBytes, _ := hex.DecodeString(fp)
					var fp16 [16]byte
					copy(fp16[:], fpBytes)
					s = &types.Series{Fingerprint: fp16, Labels: map[string]string{}}
					seriesMap[fp] = s
				}
				s.Samples = append(s.Samples, types.Sample{Timestamp: ts, Value: colVal.Row(i)})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("tier query: %w", err)
	}

	matrix := make(types.Matrix, 0, len(seriesMap))
	for _, s := range seriesMap {
		matrix = append(matrix, *s)
	}

	slog.Debug("tier_query",
		"rows", totalRows,
		"series", len(matrix),
		"duration", time.Since(t0),
	)
	return matrix, nil
}

// CounterBucket holds per-bucket counter data.
type CounterBucket struct {
	Timestamp    int64
	CounterTotal float64
	FirstTime    int64
	LastTime     int64
}

// CounterSeries holds per-bucket counter data for one fingerprint.
type CounterSeries struct {
	Labels  map[string]string
	Buckets []CounterBucket
}

// ExecTierQueryRaw executes a counter tier query.
func (p *Pool) ExecTierQueryRaw(ctx context.Context, sql string, fps []string) (map[string]*CounterSeries, error) {
	t0 := time.Now()
	totalRows := 0
	seriesMap := make(map[string]*CounterSeries)

	var (
		colFP  proto.ColStr
		colTS  proto.ColInt64
		colVal proto.ColFloat64
		colFT  proto.ColInt64
		colLT  proto.ColInt64
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: sql,
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "step_ts", Data: &colTS},
			{Name: "value", Data: &colVal},
			{Name: "ft", Data: &colFT},
			{Name: "lt", Data: &colLT},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			totalRows += block.Rows
			for i := 0; i < block.Rows; i++ {
				fp := colFP.Row(i)
				s, ok := seriesMap[fp]
				if !ok {
					s = &CounterSeries{Labels: map[string]string{}}
					seriesMap[fp] = s
				}
				s.Buckets = append(s.Buckets, CounterBucket{
					Timestamp:    colTS.Row(i),
					CounterTotal: colVal.Row(i),
					FirstTime:    colFT.Row(i),
					LastTime:     colLT.Row(i),
				})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("tier query raw: %w", err)
	}
	slog.Debug("tier_query_raw", "rows", totalRows, "series", len(seriesMap), "duration", time.Since(t0))
	return seriesMap, nil
}

// GaugeBucket holds per-bucket gauge aggregates.
type GaugeBucket struct {
	Timestamp int64
	ValSum    float64
	ValCount  uint64
	ValMin    float64
	ValMax    float64
}

// GaugeSeries holds per-bucket gauge data for one fingerprint.
type GaugeSeries struct {
	Labels  map[string]string
	Buckets []GaugeBucket
}

// ExecGaugeQuery executes a gauge tier query.
func (p *Pool) ExecGaugeQuery(ctx context.Context, sql string, fps []string) (map[string]*GaugeSeries, error) {
	t0 := time.Now()
	totalRows := 0
	seriesMap := make(map[string]*GaugeSeries)

	var (
		colFP       proto.ColStr
		colTS       proto.ColInt64
		colValSum   proto.ColFloat64
		colValCount proto.ColUInt64
		colValMin   proto.ColFloat64
		colValMax   proto.ColFloat64
	)

	err := p.pool.Do(ctx, ch.Query{
		Body: sql,
		Result: proto.Results{
			{Name: "fingerprint", Data: &colFP},
			{Name: "step_ts", Data: &colTS},
			{Name: "val_sum", Data: &colValSum},
			{Name: "val_count", Data: &colValCount},
			{Name: "val_min", Data: &colValMin},
			{Name: "val_max", Data: &colValMax},
		},
		OnResult: func(_ context.Context, block proto.Block) error {
			totalRows += block.Rows
			for i := 0; i < block.Rows; i++ {
				fp := colFP.Row(i)
				s, ok := seriesMap[fp]
				if !ok {
					s = &GaugeSeries{Labels: map[string]string{}}
					seriesMap[fp] = s
				}
				s.Buckets = append(s.Buckets, GaugeBucket{
					Timestamp: colTS.Row(i),
					ValSum:    colValSum.Row(i),
					ValCount:  colValCount.Row(i),
					ValMin:    colValMin.Row(i),
					ValMax:    colValMax.Row(i),
				})
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gauge tier query: %w", err)
	}
	slog.Debug("gauge_tier_query", "rows", totalRows, "series", len(seriesMap), "duration", time.Since(t0))
	return seriesMap, nil
}

// Exec executes a raw SQL statement via HTTP (DDL, ALTER, etc.)
func (p *Pool) Exec(ctx context.Context, sql string) error {
	u := strings.TrimRight(p.httpAddr, "/") + "/?database=" + p.Schema.Database
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	if err != nil {
		return err
	}
	if p.httpUser != "" {
		req.SetBasicAuth(p.httpUser, p.httpPassword)
	}
	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// QueryRow executes a query and scans the first row into dest.
func (p *Pool) QueryRow(ctx context.Context, sql string, dest ...interface{}) error {
	if len(dest) == 1 {
		if ptr, ok := dest[0].(*uint64); ok {
			var col proto.ColUInt64
			err := p.pool.Do(ctx, ch.Query{
				Body:   sql,
				Result: proto.Results{{Name: "count()", Data: &col}},
				OnResult: func(_ context.Context, block proto.Block) error {
					if block.Rows > 0 {
						*ptr = col.Row(0)
					}
					return nil
				},
			})
			return err
		}
		if ptr, ok := dest[0].(*string); ok {
			var col proto.ColStr
			err := p.pool.Do(ctx, ch.Query{
				Body:   sql,
				Result: proto.Results{{Name: "checksum", Data: &col}},
				OnResult: func(_ context.Context, block proto.Block) error {
					if block.Rows > 0 {
						*ptr = col.Row(0)
					}
					return nil
				},
			})
			return err
		}
	}
	return fmt.Errorf("QueryRow: unsupported dest type")
}

// extractParam gets a param value from QueryParams by name (without param_ prefix).
func extractParam(params *chclient.QueryParams, name string) string {
	if params == nil {
		return ""
	}
	return params.URLValues().Get("param_" + name)
}

// extractMatchers extracts label matchers (lk0/lv0/lo0, lk1/lv1/lo1...) from params.
func extractMatchers(params *chclient.QueryParams) []LabelMatcher {
	if params == nil {
		return nil
	}
	vals := params.URLValues()
	var matchers []LabelMatcher
	for i := 0; ; i++ {
		is := strconv.Itoa(i)
		lk := vals.Get("param_lk" + is)
		lv := vals.Get("param_lv" + is)
		lo := vals.Get("param_lo" + is)
		if lk == "" {
			break
		}
		if lo == "" {
			lo = "="
		}
		matchers = append(matchers, LabelMatcher{Name: lk, Op: lo, Value: lv})
	}
	return matchers
}

// inlineParams replaces {name:Type} placeholders with actual values.
func inlineParams(sql string, params *chclient.QueryParams) string {
	if params == nil {
		return sql
	}
	result := sql
	for key, values := range params.URLValues() {
		if len(values) == 0 {
			continue
		}
		name := key
		if strings.HasPrefix(name, "param_") {
			name = name[6:]
		}
		val := values[0]
		strP := "{" + name + ":String}"
		intP := "{" + name + ":Int64}"
		if strings.Contains(result, strP) {
			result = strings.ReplaceAll(result, strP, "'"+strings.ReplaceAll(val, "'", "\\'")+"'")
		}
		if strings.Contains(result, intP) {
			result = strings.ReplaceAll(result, intP, val)
		}
	}
	return result
}

// buildFingerprintIN builds "AND fingerprint IN (unhex('aa...'), unhex('bb...'))"
// for FixedString(16) hex-encoded fingerprints.
func buildFingerprintIN(fps []string, fpColumn string) string {
	if len(fps) == 0 {
		return ""
	}
	wrapped := make([]string, len(fps))
	for i, fp := range fps {
		wrapped[i] = "unhex('" + fp + "')"
	}
	return "AND " + fpColumn + " IN (" + strings.Join(wrapped, ",") + ")"
}

// queryJSON executes a SQL query via HTTP JSONEachRow and decodes the first row into dest.
func (p *Pool) queryJSON(ctx context.Context, sql string, dest interface{}) error {
	u := strings.TrimRight(p.httpAddr, "/") + "/?database=" + p.Schema.Database + "&default_format=JSONEachRow"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	if err != nil {
		return err
	}
	if p.httpUser != "" {
		req.SetBasicAuth(p.httpUser, p.httpPassword)
	}
	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("queryJSON: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("queryJSON HTTP %d: %s", resp.StatusCode, string(body))
	}
	dec := json.NewDecoder(resp.Body)
	if dec.More() {
		return dec.Decode(dest)
	}
	return nil
}

// Ping establishes a connection and warms up the pool.
func (p *Pool) Ping(ctx context.Context) error {
	var col proto.ColUInt8
	return p.pool.Do(ctx, ch.Query{
		Body:   "SELECT 1",
		Result: proto.Results{{Name: "1", Data: &col}},
		OnResult: func(_ context.Context, _ proto.Block) error { return nil },
	})
}

// Close closes the pool.
func (p *Pool) Close() {
	p.pool.Close()
}
