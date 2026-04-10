package clickhouse

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/prompb"

	"github.com/PromClick/PromClick/fingerprint"
)

// WriterConfig holds configuration for the batch writer.
type WriterConfig struct {
	Database      string
	BatchSize     int
	QueueSize     int
	FlushInterval time.Duration
}

// Writer batches incoming remote_write data and flushes to ClickHouse.
type Writer struct {
	pool *Pool
	cfg  WriterConfig
	queue chan writeBatch
}

type writeBatch struct {
	series []prompb.TimeSeries
}

// NewWriter creates a Writer with a background flush goroutine.
func NewWriter(pool *Pool, cfg WriterConfig) *Writer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10000
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	return &Writer{
		pool:  pool,
		cfg:   cfg,
		queue: make(chan writeBatch, cfg.QueueSize),
	}
}

// Start begins the background flush goroutine.
func (w *Writer) Start(ctx context.Context) {
	go w.run(ctx)
}

// Write enqueues a write request for batched insertion.
func (w *Writer) Write(ctx context.Context, req *prompb.WriteRequest) error {
	select {
	case w.queue <- writeBatch{series: req.Timeseries}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) run(ctx context.Context) {
	batch := make([]prompb.TimeSeries, 0, w.cfg.BatchSize)
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case b := <-w.queue:
			batch = append(batch, b.series...)
			if len(batch) >= w.cfg.BatchSize {
				w.flush(ctx, batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				w.flush(ctx, batch)
				batch = batch[:0]
			}
		case <-ctx.Done():
			if len(batch) > 0 {
				w.flush(context.Background(), batch)
			}
			return
		}
	}
}

// tsRow holds all data for one unique label-set (fingerprint) in a write batch.
type tsRow struct {
	fp        [16]byte
	tagNames  []string
	tagValues []string
	// values__float64 entries: (poll_epoch_ns, value, field)
	samples [][3]interface{} // [int64, float64, string]
}

func (w *Writer) flush(ctx context.Context, series []prompb.TimeSeries) {
	t0 := time.Now()

	// Group samples by fingerprint (all labels INCLUDING __name__)
	type key = [16]byte
	rowMap := make(map[key]*tsRow, len(series))

	for i := range series {
		ts := &series[i]

		// Build full label map including __name__
		labels := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			labels[l.Name] = l.Value
		}

		fp := fingerprint.Compute(labels)

		row, ok := rowMap[fp]
		if !ok {
			// Build sorted tag_names/tag_values (deterministic order)
			keys := make([]string, 0, len(labels))
			for k := range labels {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			names := make([]string, len(keys))
			values := make([]string, len(keys))
			for i, k := range keys {
				names[i] = k
				values[i] = labels[k]
			}

			row = &tsRow{
				fp:        fp,
				tagNames:  names,
				tagValues: values,
			}
			rowMap[fp] = row
		}

		for _, s := range ts.Samples {
			// Prometheus timestamps are in milliseconds; convert to nanoseconds.
			nsTs := s.Timestamp * 1_000_000
			row.samples = append(row.samples, [3]interface{}{nsTs, s.Value, ""})
		}
	}

	if len(rowMap) == 0 {
		return
	}

	if err := w.insertRows(ctx, rowMap); err != nil {
		slog.Error("write: insert __ts failed", "error", err, "rows", len(rowMap))
	}

	slog.Debug("write: flush",
		"series", len(rowMap),
		"duration", time.Since(t0),
	)
}

// insertRows inserts all rows into data__tagset.__ts via HTTP VALUES INSERT.
// The Null engine fires the materialized views which fan out to the real tables.
func (w *Writer) insertRows(ctx context.Context, rows map[[16]byte]*tsRow) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(w.cfg.Database)
	sb.WriteString(".__ts ")
	sb.WriteString("(insert_ts, source, fingerprint, tag_names, tag_values, ")
	sb.WriteString("values__float64, values__boolean, values__int64, values__string, values__uint64) ")
	sb.WriteString("VALUES\n")

	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	first := true
	for _, row := range rows {
		if !first {
			sb.WriteString(",\n")
		}
		first = false

		fmt.Fprintf(&sb, "('%s', 'prometheus', unhex('%s'), %s, %s, %s, [], [], [], [])",
			now,
			hex.EncodeToString(row.fp[:]),
			sqlStringArray(row.tagNames),
			sqlStringArray(row.tagValues),
			sqlFloat64Tuples(row.samples),
		)
	}

	return w.execHTTP(ctx, sb.String())
}

// execHTTP sends a SQL statement to ClickHouse via HTTP POST.
func (w *Writer) execHTTP(ctx context.Context, sql string) error {
	u := strings.TrimRight(w.pool.httpAddr, "/") + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	if err != nil {
		return err
	}
	if w.pool.httpUser != "" {
		req.SetBasicAuth(w.pool.httpUser, w.pool.httpPassword)
	}
	resp, err := w.pool.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// sqlStringArray formats a []string as a ClickHouse array literal: ['v1','v2']
func sqlStringArray(ss []string) string {
	if len(ss) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, s := range ss {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('\'')
		b.WriteString(strings.ReplaceAll(s, "'", "\\'"))
		b.WriteByte('\'')
	}
	b.WriteByte(']')
	return b.String()
}

// sqlFloat64Tuples formats samples as [(ns, val, field), ...] for values__float64.
func sqlFloat64Tuples(samples [][3]interface{}) string {
	if len(samples) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, s := range samples {
		if i > 0 {
			b.WriteByte(',')
		}
		ns := s[0].(int64)
		val := s[1].(float64)
		field := s[2].(string)
		fmt.Fprintf(&b, "(%d,%g,'%s')", ns, val, strings.ReplaceAll(field, "'", "\\'"))
	}
	b.WriteByte(']')
	return b.String()
}
