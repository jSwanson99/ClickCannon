package generate

import (
	"context"
	"testing"

	"github.com/ClickHouse/ClickCannon/internal/block"

	"github.com/ClickHouse/ch-go/proto"
)

// blockVersion matches the protocol version the disk reader decodes with.
const blockVersion = 54451

// TestProfilesGenDiskRoundTrip generates a profiles block, encodes it as a
// Native block, and decodes it back into the disk-path columns. This proves the
// generate and disk column sets agree on column names, order, and types (a decode
// fails otherwise) and that values survive the round-trip.
func TestProfilesGenDiskRoundTrip(t *testing.T) {
	p, err := GetProfile("otel_demo")
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}

	pcfg := ProfilesConfig{SamplesPerProfileMin: 20, SamplesPerProfileMax: 50, StackDepthMin: 3, StackDepthMax: 12, DurationMinMs: 1000, DurationMaxMs: 5000, PeriodNs: 10000000}
	filler := NewProfilesFiller(p, pcfg)

	gen := NewGenProfilesColumns()
	rng := NewRng("profiles-test", 0)
	const n = 200
	if got := filler.Fill(context.Background(), rng, gen, n); got != n {
		t.Fatalf("Fill wrote %d rows, want %d", got, n)
	}

	genInput := gen.Input()
	var buf proto.Buffer
	encBlock := proto.Block{Columns: len(genInput), Rows: n}
	if err := encBlock.EncodeRawBlock(&buf, blockVersion, genInput); err != nil {
		t.Fatalf("EncodeRawBlock: %v", err)
	}

	disk := block.NewProfilesSharedColumns()
	var decBlock proto.Block
	if err := decBlock.DecodeRawBlock(buf.Reader(), blockVersion, disk.Results()); err != nil {
		t.Fatalf("DecodeRawBlock: %v", err)
	}
	if decBlock.Rows != n {
		t.Fatalf("decoded %d rows, want %d", decBlock.Rows, n)
	}

	// Column name/order parity between the two paths.
	if genCols, diskCols := genInput.Columns(), disk.Input().Columns(); genCols != diskCols {
		t.Fatalf("column mismatch:\n gen:  %s\n disk: %s", genCols, diskCols)
	}

	// Samples are grouped into profiles: many rows share a ProfileId.
	distinct := map[string]struct{}{}
	for i := 0; i < n; i++ {
		distinct[gen.ProfileID.Row(i)] = struct{}{}
	}
	if len(distinct) >= n {
		t.Fatalf("expected samples grouped under shared ProfileIds, got %d distinct across %d rows", len(distinct), n)
	}

	// Spot-check values across a scalar, low-cardinality, numeric, and array column.
	res := disk.Results()
	profileID := findColumn(t, res, "ProfileId").(*proto.ColStr)
	sampleType := findColumn(t, res, "SampleType").(proto.Column)
	stackHash := findColumn(t, res, "StackHash").(*proto.ColUInt64)
	addresses := findColumn(t, res, "Addresses").(*proto.ColArr[uint64])
	// FunctionNames is Array(LowCardinality(String)); the disk wrapper's element
	// Row panics by design, so only its length is checked here.
	functionNames := findColumn(t, res, "FunctionNames").(*proto.ColArr[string])

	for i := 0; i < n; i++ {
		if got, want := profileID.Row(i), gen.ProfileID.Row(i); got != want {
			t.Fatalf("row %d ProfileId = %q, want %q", i, got, want)
		}
		if got, want := block.ColStrRow(sampleType, i), gen.SampleType.Row(i); got != want {
			t.Fatalf("row %d SampleType = %q, want %q", i, got, want)
		}
		if got, want := (*stackHash)[i], gen.StackHash[i]; got != want {
			t.Fatalf("row %d StackHash = %d, want %d", i, got, want)
		}
		if got, want := addresses.Row(i), gen.Addresses.Row(i); !equalUint64s(got, want) {
			t.Fatalf("row %d Addresses = %v, want %v", i, got, want)
		}
		if got, want := functionNames.RowLen(i), len(gen.FunctionNames.Row(i)); got != want {
			t.Fatalf("row %d FunctionNames length = %d, want %d", i, got, want)
		}
	}
}

func findColumn(t *testing.T, res proto.Results, name string) proto.ColResult {
	t.Helper()
	for _, c := range res {
		if c.Name == name {
			return c.Data
		}
	}
	t.Fatalf("column %q not found in results", name)
	return nil
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
