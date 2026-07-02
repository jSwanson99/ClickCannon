package otel

import (
	"clickcannon/internal/block"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

// logsBuilder groups log records by (resource, scope) fingerprint into OTLP
// ResourceLogs so records sharing a resource+scope are emitted together.
type logsBuilder struct {
	groups map[uint64]*logspb.ResourceLogs
	order  []*logspb.ResourceLogs
	count  int
}

func newLogsBuilder() *logsBuilder {
	return &logsBuilder{groups: make(map[uint64]*logspb.ResourceLogs)}
}

func (b *logsBuilder) len() int { return b.count }

func (b *logsBuilder) add(r *block.LogRow) {
	// Fold a distinct delimiter after each scalar field so adjacent values with
	// ambiguous boundaries ("ab"+"c" vs "a"+"bc") cannot collide. A residual
	// 64-bit hash collision (~2^-64 per pair) would merge two groups; that is an
	// accepted tradeoff for a load generator (affects only grouping metadata).
	key := fnvStr(fnvOffset64, r.ServiceName)
	key = (key ^ 0x01) * fnvPrime64
	key = fnvStr(key, r.ResourceSchemaURL)
	key = (key ^ 0x02) * fnvPrime64
	key = fnvKVs(key, r.ResourceAttrs)
	key = (key ^ 0x2d) * fnvPrime64
	key = fnvStr(key, r.ScopeName)
	key = (key ^ 0x03) * fnvPrime64
	key = fnvStr(key, r.ScopeVersion)
	key = (key ^ 0x04) * fnvPrime64
	key = fnvStr(key, r.ScopeSchemaURL)
	key = (key ^ 0x05) * fnvPrime64
	key = fnvKVs(key, r.ScopeAttrs)

	rl := b.groups[key]
	if rl == nil {
		rl = &logspb.ResourceLogs{
			Resource:  buildResource(r.ServiceName, r.ResourceAttrs),
			SchemaUrl: r.ResourceSchemaURL,
			ScopeLogs: []*logspb.ScopeLogs{{
				Scope: &commonpb.InstrumentationScope{
					Name:       r.ScopeName,
					Version:    r.ScopeVersion,
					Attributes: attrsFromKV(r.ScopeAttrs),
				},
				SchemaUrl: r.ScopeSchemaURL,
			}},
		}
		b.groups[key] = rl
		b.order = append(b.order, rl)
	}

	ts := nano(r.Timestamp)
	rl.ScopeLogs[0].LogRecords = append(rl.ScopeLogs[0].LogRecords, &logspb.LogRecord{
		TimeUnixNano:         ts,
		ObservedTimeUnixNano: ts,
		SeverityNumber:       logspb.SeverityNumber(r.SeverityNumber),
		SeverityText:         r.SeverityText,
		Body:                 stringValue(r.Body),
		Attributes:           attrsFromKV(r.LogAttrs),
		Flags:                r.TraceFlags,
		TraceId:              decodeID(r.TraceID),
		SpanId:               decodeID(r.SpanID),
	})
	b.count++
}

func (b *logsBuilder) build() *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{ResourceLogs: b.order}
}

func (b *logsBuilder) reset() {
	clear(b.groups)
	b.order = b.order[:0]
	b.count = 0
}
