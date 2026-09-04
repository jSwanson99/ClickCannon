package otel

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestSignalURL(t *testing.T) {
	cases := []struct {
		raw       string
		insecure  bool
		wantLogs  string
		wantTrace string
	}{
		{"https://localhost:4318", false, "https://localhost:4318/v1/logs", "https://localhost:4318/v1/traces"},
		{"localhost:4318", true, "http://localhost:4318/v1/logs", "http://localhost:4318/v1/traces"},
		{"localhost:4318", false, "https://localhost:4318/v1/logs", "https://localhost:4318/v1/traces"},
		{"http://localhost:4318/", false, "http://localhost:4318/v1/logs", "http://localhost:4318/v1/traces"},
		{"https://collector.example.com/otlp", false, "https://collector.example.com/otlp/v1/logs", "https://collector.example.com/otlp/v1/traces"},
		// An explicit scheme wins over the insecure flag.
		{"https://localhost:4318", true, "https://localhost:4318/v1/logs", "https://localhost:4318/v1/traces"},
	}
	for _, c := range cases {
		base, err := httpBaseURL(c.raw, c.insecure)
		if err != nil {
			t.Fatalf("httpBaseURL(%q): %v", c.raw, err)
		}
		if got := signalURL(base, "/v1/logs"); got != c.wantLogs {
			t.Errorf("signalURL(%q) logs = %q, want %q", c.raw, got, c.wantLogs)
		}
		if got := signalURL(base, "/v1/traces"); got != c.wantTrace {
			t.Errorf("signalURL(%q) traces = %q, want %q", c.raw, got, c.wantTrace)
		}
	}
}

func TestHTTPExportLogsRoundTrip(t *testing.T) {
	for _, useGzip := range []bool{false, true} {
		var gotPath, gotContentType, gotEncoding, gotAuth string
		var gotReq collogspb.ExportLogsServiceRequest

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotContentType = r.Header.Get("Content-Type")
			gotEncoding = r.Header.Get("Content-Encoding")
			gotAuth = r.Header.Get("Authorization")

			var body io.Reader = r.Body
			if gotEncoding == "gzip" {
				zr, err := gzip.NewReader(r.Body)
				if err != nil {
					t.Errorf("gzip.NewReader: %v", err)
					http.Error(w, "bad gzip", http.StatusBadRequest)
					return
				}
				body = zr
			}
			raw, _ := io.ReadAll(body)
			if err := proto.Unmarshal(raw, &gotReq); err != nil {
				t.Errorf("proto.Unmarshal: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		compression := ""
		if useGzip {
			compression = "gzip"
		}
		cfg := &Config{
			Protocol:    protocolHTTP,
			URL:         srv.URL,
			Compression: compression,
			Headers:     map[string]string{"Authorization": "Bearer tok"},
		}
		c, err := dialHTTP(cfg)
		if err != nil {
			t.Fatalf("dialHTTP: %v", err)
		}

		req := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{{
				ScopeLogs: []*logspb.ScopeLogs{{
					LogRecords: []*logspb.LogRecord{{TimeUnixNano: 42}},
				}},
			}},
		}
		if err := c.exportLogs(context.Background(), req); err != nil {
			t.Fatalf("exportLogs (gzip=%v): %v", useGzip, err)
		}

		if gotPath != "/v1/logs" {
			t.Errorf("path = %q, want /v1/logs", gotPath)
		}
		if gotContentType != "application/x-protobuf" {
			t.Errorf("content-type = %q", gotContentType)
		}
		if useGzip && gotEncoding != "gzip" {
			t.Errorf("content-encoding = %q, want gzip", gotEncoding)
		}
		if !useGzip && gotEncoding != "" {
			t.Errorf("content-encoding = %q, want empty", gotEncoding)
		}
		if gotAuth != "Bearer tok" {
			t.Errorf("authorization = %q", gotAuth)
		}
		if len(gotReq.ResourceLogs) != 1 ||
			gotReq.ResourceLogs[0].ScopeLogs[0].LogRecords[0].TimeUnixNano != 42 {
			t.Errorf("decoded request mismatch: %+v", &gotReq)
		}
	}
}

func TestHTTPExportNon2xxErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c, err := dialHTTP(&Config{Protocol: protocolHTTP, URL: srv.URL})
	if err != nil {
		t.Fatalf("dialHTTP: %v", err)
	}
	err = c.exportLogs(context.Background(), &collogspb.ExportLogsServiceRequest{})
	if err == nil {
		t.Fatal("expected error on 503 response, got nil")
	}
}

func TestHTTPExportTracesRoundTrip(t *testing.T) {
	var gotPath string
	var gotReq coltracepb.ExportTraceServiceRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		if err := proto.Unmarshal(raw, &gotReq); err != nil {
			t.Errorf("proto.Unmarshal: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := dialHTTP(&Config{Protocol: protocolHTTP, URL: srv.URL})
	if err != nil {
		t.Fatalf("dialHTTP: %v", err)
	}

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{Name: "span-a", StartTimeUnixNano: 7}},
			}},
		}},
	}
	if err := c.exportTraces(context.Background(), req); err != nil {
		t.Fatalf("exportTraces: %v", err)
	}

	if gotPath != "/v1/traces" {
		t.Errorf("path = %q, want /v1/traces", gotPath)
	}
	if len(gotReq.ResourceSpans) != 1 || gotReq.ResourceSpans[0].ScopeSpans[0].Spans[0].Name != "span-a" {
		t.Errorf("decoded request mismatch: %+v", &gotReq)
	}
}

// dial must pick the transport from the protocol field, and default to gRPC so
// configs written before the field existed keep their behaviour.
func TestDialSelectsTransport(t *testing.T) {
	cases := []struct {
		protocol string
		url      string
		wantHTTP bool
	}{
		{"", "localhost:4317", false},
		{protocolGRPC, "localhost:4317", false},
		{protocolHTTP, "http://localhost:4318", true},
	}
	for _, c := range cases {
		e, err := dial(&Config{Protocol: c.protocol, URL: c.url, Insecure: true})
		if err != nil {
			t.Fatalf("dial(protocol=%q): %v", c.protocol, err)
		}
		_, isHTTP := e.(*httpClient)
		if isHTTP != c.wantHTTP {
			t.Errorf("dial(protocol=%q) returned %T, wantHTTP=%v", c.protocol, e, c.wantHTTP)
		}
		if err := e.close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}
}

func TestValidateProtocol(t *testing.T) {
	base := Config{Enabled: true, URL: "localhost:4318", Threads: 1, BatchSize: 1}

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"empty defaults to grpc", func(c *Config) {}, false},
		{"grpc", func(c *Config) { c.Protocol = protocolGRPC }, false},
		{"http", func(c *Config) { c.Protocol = protocolHTTP }, false},
		{"http/protobuf is not accepted", func(c *Config) { c.Protocol = "http/protobuf" }, true},
		{"http/json is not accepted", func(c *Config) { c.Protocol = "http/json" }, true},
		{"unknown", func(c *Config) { c.Protocol = "thrift" }, true},
		// A url that only dialHTTP would reject must fail validation, otherwise
		// the scheduler retries the bad config forever as a transient failure.
		{"http rejects a grpc scheme", func(c *Config) {
			c.Protocol = protocolHTTP
			c.URL = "grpc://localhost:4317"
		}, true},
		{"grpc tolerates a grpc scheme", func(c *Config) {
			c.Protocol = protocolGRPC
			c.URL = "grpc://localhost:4317"
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.wantErr && err == nil {
				t.Errorf("Validate(%+v) = nil, want error", cfg.Protocol)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Validate(%q) = %v, want nil", cfg.Protocol, err)
			}
		})
	}
}
