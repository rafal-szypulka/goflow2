// Package http implements an HTTP transport for ndjson payloads.
package http

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/netsampler/goflow2/v2/transport"
)

type headerList []string

func (h *headerList) String() string {
	return strings.Join(*h, ",")
}

func (h *headerList) Set(value string) error {
	if value == "" {
		return fmt.Errorf("header value cannot be empty")
	}
	*h = append(*h, value)
	return nil
}

// HTTPDriver sends formatted messages to an HTTP endpoint.
type HTTPDriver struct {
	urlRaw        string
	lineSeparator string
	timeout       time.Duration
	gzipEnabled   bool
	headers       headerList
	batchSize     int
	batchInterval time.Duration
	queueSize     int
	workers       int
	debug         bool

	client  *http.Client
	target  *url.URL
	headerM http.Header

	buffer  bytes.Buffer
	lock    sync.Mutex
	stopCh  chan struct{}
	queue   chan []byte
	wg      sync.WaitGroup
}

// Prepare registers flags for HTTP transport configuration.
func (d *HTTPDriver) Prepare() error {
	flag.StringVar(&d.urlRaw, "transport.http.url", "", "HTTP endpoint URL")
	flag.DurationVar(&d.timeout, "transport.http.timeout", 10*time.Second, "HTTP transport timeout")
	flag.BoolVar(&d.gzipEnabled, "transport.http.gzip", true, "Enable gzip compression")
	flag.StringVar(&d.lineSeparator, "transport.http.sep", "\n", "Line separator appended to each payload")
	flag.IntVar(&d.batchSize, "transport.http.batch.size", 1024*1024, "Batch size in bytes before sending")
	flag.DurationVar(&d.batchInterval, "transport.http.batch.interval", 5*time.Second, "Batch flush interval")
	flag.IntVar(&d.queueSize, "transport.http.queue.size", 1024, "Queue size for pending HTTP batches")
	flag.IntVar(&d.workers, "transport.http.workers", 1, "Number of HTTP sender workers")
	flag.BoolVar(&d.debug, "transport.http.debug", false, "Enable debug logging for HTTP transport")
	flag.Var(&d.headers, "transport.http.header", "Extra HTTP header (repeatable, format: 'Key: Value')")
	return nil
}

// Init validates configuration and prepares the HTTP client.
func (d *HTTPDriver) Init() error {
	if d.urlRaw == "" {
		return fmt.Errorf("transport.http.url is required")
	}
	target, err := url.Parse(d.urlRaw)
	if err != nil {
		return fmt.Errorf("invalid transport.http.url: %w", err)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return fmt.Errorf("transport.http.url must use http or https")
	}
	if target.Host == "" {
		return fmt.Errorf("transport.http.url must include a host")
	}
	headerM := make(http.Header)
	for _, header := range d.headers {
		parts := strings.SplitN(header, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid transport.http.header %q", header)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return fmt.Errorf("invalid transport.http.header %q", header)
		}
		headerM.Add(key, value)
	}

	d.target = target
	d.client = &http.Client{Timeout: d.timeout}
	d.headerM = headerM

	if d.batchSize <= 0 && d.batchInterval <= 0 {
		return nil
	}

	if d.workers <= 0 {
		d.workers = 1
	}
	if d.queueSize <= 0 {
		d.queueSize = 1
	}

	d.stopCh = make(chan struct{})
	d.queue = make(chan []byte, d.queueSize)
	d.wg.Add(1)
	go d.batchLoop()
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go d.sendLoop()
	}
	return nil
}

// Send queues or posts a formatted message as ndjson with optional gzip compression.
func (d *HTTPDriver) Send(key, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if d.batchSize <= 0 && d.batchInterval <= 0 {
		return d.sendRequest(appendWithSeparator(data, d.lineSeparator))
	}

	d.lock.Lock()
	d.buffer.Write(data)
	if d.lineSeparator != "" {
		d.buffer.WriteString(d.lineSeparator)
	}
	shouldFlush := d.batchSize > 0 && d.buffer.Len() >= d.batchSize
	payload := []byte(nil)
	if shouldFlush {
		payload = make([]byte, d.buffer.Len())
		copy(payload, d.buffer.Bytes())
		d.buffer.Reset()
	}
	d.lock.Unlock()
	if shouldFlush {
		return d.enqueue(payload)
	}
	return nil
}

func (d *HTTPDriver) batchLoop() {
	defer d.wg.Done()

	var ticker *time.Ticker
	if d.batchInterval > 0 {
		ticker = time.NewTicker(d.batchInterval)
		defer ticker.Stop()
	}

	for {
		var tick <-chan time.Time
		if ticker != nil {
			tick = ticker.C
		}
		select {
		case <-d.stopCh:
			return
		case <-tick:
			d.flush()
		}
	}
}

func (d *HTTPDriver) flush() {
	d.lock.Lock()
	if d.buffer.Len() == 0 {
		d.lock.Unlock()
		return
	}
	if d.debug {
		slog.Debug("http transport flushing batch buffer",
			slog.Int("buffer_size_bytes", d.buffer.Len()),
		)
	}
	payload := make([]byte, d.buffer.Len())
	copy(payload, d.buffer.Bytes())
	d.buffer.Reset()
	d.lock.Unlock()
	_ = d.enqueue(payload)
}

func appendWithSeparator(data []byte, sep string) []byte {
	if sep == "" {
		cp := make([]byte, len(data))
		copy(cp, data)
		return cp
	}
	out := make([]byte, 0, len(data)+len(sep))
	out = append(out, data...)
	out = append(out, []byte(sep)...)
	return out
}

func (d *HTTPDriver) enqueue(payload []byte) error {
	if d.queue == nil {
		return d.sendRequest(payload)
	}
	select {
	case d.queue <- payload:
		if d.debug {
			slog.Debug("http transport enqueued batch",
				slog.Int("batch_size_bytes", len(payload)),
				slog.Int("queue_depth", len(d.queue)),
				slog.Int("queue_capacity", cap(d.queue)),
			)
		}
		return nil
	default:
		if d.debug {
			slog.Debug("http transport queue full",
				slog.Int("batch_size_bytes", len(payload)),
				slog.Int("queue_depth", len(d.queue)),
				slog.Int("queue_capacity", cap(d.queue)),
			)
		}
		return fmt.Errorf("http transport queue is full")
	}
}

func (d *HTTPDriver) sendLoop() {
	defer d.wg.Done()
	for payload := range d.queue {
		_ = d.sendRequest(payload)
	}
}

func (d *HTTPDriver) sendRequest(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	var body bytes.Buffer
	var writer io.Writer = &body
	var gzWriter *gzip.Writer
	if d.gzipEnabled {
		gzWriter = gzip.NewWriter(&body)
		writer = gzWriter
	}
	if _, err := writer.Write(payload); err != nil {
		return err
	}
	if gzWriter != nil {
		if err := gzWriter.Close(); err != nil {
			return err
		}
	}
	if d.debug {
		slog.Debug("http transport sending batch",
			slog.Int("batch_size_bytes", len(payload)),
			slog.Int("compressed_size_bytes", body.Len()),
		)
	}

	req, err := http.NewRequest(http.MethodPost, d.target.String(), &body)
	if err != nil {
		return err
	}
	for k, values := range d.headerM {
		for _, value := range values {
			req.Header.Add(k, value)
		}
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/x-ndjson")
	}
	if d.gzipEnabled && req.Header.Get("Content-Encoding") == "" {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("http transport status %s", resp.Status)
	}
	return nil
}

// Close releases HTTP transport resources.
func (d *HTTPDriver) Close() error {
	if d.stopCh != nil {
		close(d.stopCh)
		d.flush()
		if d.queue != nil {
			close(d.queue)
		}
		d.wg.Wait()
	}
	if d.client != nil {
		d.client.CloseIdleConnections()
	}
	return nil
}

func init() {
	d := &HTTPDriver{}
	transport.RegisterTransportDriver("http", d)
}
