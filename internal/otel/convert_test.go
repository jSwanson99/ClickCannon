package otel

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ClickCannon/internal/block"
	"github.com/ClickHouse/ClickCannon/internal/generate"

	"github.com/ClickHouse/ch-go/proto"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	gproto "google.golang.org/protobuf/proto"
)

// TestConvertLogsFromGenerate runs generated log data through the reader and OTLP
// builder end-to-end and validates the resulting request is well-formed.
func TestConvertLogsFromGenerate(t *testing.T) {
	prof, err := generate.GetProfile(generate.DefaultProfile)
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}
	filler := generate.NewLogsFiller(prof)
	cols := generate.NewGenLogsColumns()
	rng := generate.NewRng("seed", 0)

	const rows = 200
	if n := filler.Fill(context.Background(), rng, cols, rows); n != rows {
		t.Fatalf("Fill wrote %d rows, want %d", n, rows)
	}
	if cols.Rows() != rows {
		t.Fatalf("Rows() = %d, want %d", cols.Rows(), rows)
	}

	b := newLogsBuilder()
	var row block.LogRow
	for i := 0; i < cols.Rows(); i++ {
		cols.ReadLogRow(i, &row)
		b.add(&row)
	}
	if b.len() != rows {
		t.Fatalf("builder len = %d, want %d", b.len(), rows)
	}

	req := b.build()
	total := 0
	for _, rl := range req.ResourceLogs {
		if rl.Resource == nil {
			t.Fatal("resource logs missing resource")
		}
		if !hasAttr(t, rl.Resource.Attributes, "service.name") {
			t.Error("resource missing service.name attribute")
		}
		for _, sl := range rl.ScopeLogs {
			if sl.Scope == nil {
				t.Fatal("scope logs missing scope")
			}
			for _, rec := range sl.LogRecords {
				total++
				if l := len(rec.TraceId); l != 16 {
					t.Errorf("trace id len = %d, want 16", l)
				}
				if l := len(rec.SpanId); l != 8 {
					t.Errorf("span id len = %d, want 8", l)
				}
				if rec.Body == nil || rec.Body.GetStringValue() == "" {
					t.Error("log record body missing")
				}
				if rec.TimeUnixNano == 0 {
					t.Error("log record timestamp is zero")
				}
			}
		}
	}
	if total != rows {
		t.Fatalf("total records across groups = %d, want %d", total, rows)
	}

	if _, err := gproto.Marshal(req); err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	// A second batch must reset cleanly.
	b.reset()
	if b.len() != 0 {
		t.Fatalf("after reset len = %d, want 0", b.len())
	}
}

// TestConvertTracesWithEventsAndLinks hand-builds a span carrying an event and a
// link to exercise the array reading + conversion paths.
func TestConvertTracesWithEventsAndLinks(t *testing.T) {
	cols := generate.NewGenTracesColumns()
	now := time.Unix(1700000000, 123).UTC()

	traceID := "0123456789abcdef0123456789abcdef" // 32 hex -> 16 bytes
	spanID := "0123456789abcdef"                  // 16 hex -> 8 bytes
	linkTrace := "fedcba9876543210fedcba9876543210"
	linkSpan := "fedcba9876543210"

	cols.Timestamp.Data = append(cols.Timestamp.Data, proto.ToDateTime64(now, proto.PrecisionNano))
	cols.TraceID.Append(traceID)
	cols.SpanID.Append(spanID)
	cols.ParentSpanID.Append("")
	cols.TraceState.Append("")
	cols.SpanName.Append("GET /api")
	cols.SpanKind.Append("SPAN_KIND_SERVER")
	cols.ServiceName.Append("checkout")
	cols.ResourceAttributes.Append(map[string]string{"host.name": "node-1"})
	cols.ScopeName.Append("scope")
	cols.ScopeVersion.Append("1.2.3")
	cols.SpanAttributes.Append(map[string]string{"http.method": "GET"})
	cols.Duration = append(cols.Duration, 1500) // ns
	cols.StatusCode.Append("STATUS_CODE_OK")
	cols.StatusMessage.Append("ok")
	cols.EventsTimestamps.Append([]proto.DateTime64{proto.ToDateTime64(now, proto.PrecisionNano)})
	cols.EventsNames.Append([]string{"exception"})
	cols.EventsAttributes.Append([]map[string]string{{"exception.type": "BoomError"}})
	cols.LinksTraceIDs.Append([]string{linkTrace})
	cols.LinksSpanIDs.Append([]string{linkSpan})
	cols.LinksTraceStates.Append([]string{""})
	cols.LinksAttributes.Append([]map[string]string{{"link.kind": "child"}})

	if cols.Rows() != 1 {
		t.Fatalf("Rows() = %d, want 1", cols.Rows())
	}

	b := newTracesBuilder()
	var row block.TraceRow
	cols.ReadTraceRow(0, &row)
	b.add(&row)

	req := b.build()
	if len(req.ResourceSpans) != 1 {
		t.Fatalf("resource spans = %d, want 1", len(req.ResourceSpans))
	}
	rs := req.ResourceSpans[0]
	if !hasAttr(t, rs.Resource.Attributes, "service.name") || !hasAttr(t, rs.Resource.Attributes, "host.name") {
		t.Error("resource attributes incomplete")
	}
	if len(rs.ScopeSpans) != 1 || len(rs.ScopeSpans[0].Spans) != 1 {
		t.Fatalf("expected exactly one span")
	}
	span := rs.ScopeSpans[0].Spans[0]

	if len(span.TraceId) != 16 || len(span.SpanId) != 8 {
		t.Errorf("bad id lengths: trace=%d span=%d", len(span.TraceId), len(span.SpanId))
	}
	if span.Name != "GET /api" {
		t.Errorf("name = %q", span.Name)
	}
	if span.Kind.String() != "SPAN_KIND_SERVER" {
		t.Errorf("kind = %v", span.Kind)
	}
	wantStart := uint64(now.UnixNano())
	if span.StartTimeUnixNano != wantStart {
		t.Errorf("start = %d, want %d", span.StartTimeUnixNano, wantStart)
	}
	if span.EndTimeUnixNano != wantStart+1500 {
		t.Errorf("end = %d, want %d", span.EndTimeUnixNano, wantStart+1500)
	}
	if span.Status == nil || span.Status.Code.String() != "STATUS_CODE_OK" {
		t.Errorf("status = %v", span.Status)
	}
	if len(span.Events) != 1 || span.Events[0].Name != "exception" {
		t.Fatalf("events = %+v", span.Events)
	}
	if !hasAttr(t, span.Events[0].Attributes, "exception.type") {
		t.Error("event attribute missing")
	}
	if len(span.Links) != 1 || len(span.Links[0].TraceId) != 16 || len(span.Links[0].SpanId) != 8 {
		t.Fatalf("links = %+v", span.Links)
	}
	if !hasAttr(t, span.Links[0].Attributes, "link.kind") {
		t.Error("link attribute missing")
	}

	if _, err := gproto.Marshal(req); err != nil {
		t.Fatalf("marshal request: %v", err)
	}
}

func hasAttr(t *testing.T, attrs []*commonpb.KeyValue, key string) bool {
	t.Helper()
	for _, a := range attrs {
		if a.Key == key {
			return true
		}
	}
	return false
}
