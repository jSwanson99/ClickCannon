package block

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// TestLowCardRowString validates the manual key -> dictionary resolution used to
// read the custom LowCard wrapper on the disk decode path (its generic Row method
// panics by design).
func TestLowCardRowString(t *testing.T) {
	lc := newColLowCardinalityString(0, 0)
	dict := lc.Index.(*proto.ColStr)
	dict.Append("x")
	dict.Append("y")
	lc.Keys8 = append(lc.Keys8, 0, 1, 0) // rows: x, y, x

	if got := lc.Rows(); got != 3 {
		t.Fatalf("Rows() = %d, want 3", got)
	}
	want := []string{"x", "y", "x"}
	for i, w := range want {
		if got := lc.RowString(i); got != w {
			t.Errorf("RowString(%d) = %q, want %q", i, got, w)
		}
	}
}

// TestMapRowKVLowCardKey validates reading a Map(LowCardinality(String), String)
// row when the key column is the custom LowCard wrapper (disk path).
func TestMapRowKVLowCardKey(t *testing.T) {
	keyCol := newColLowCardinalityString(0, 0)
	kd := keyCol.Index.(*proto.ColStr)
	kd.Append("k1")
	kd.Append("k2")
	keyCol.Keys8 = append(keyCol.Keys8, 0, 1)

	valCol := newColString(0, 0)
	valCol.Append("v1")
	valCol.Append("v2")

	m := proto.NewMap[string, string](keyCol, &valCol)
	m.Offsets = append(m.Offsets, 2) // single map row spanning [0,2)

	kvs := MapRowKV(nil, m, 0)
	if len(kvs) != 2 {
		t.Fatalf("got %d kvs, want 2", len(kvs))
	}
	if kvs[0] != (KV{Key: "k1", Value: "v1"}) || kvs[1] != (KV{Key: "k2", Value: "v2"}) {
		t.Errorf("unexpected kvs: %+v", kvs)
	}

	// Out-of-range row must not panic and must return the input slice unchanged.
	if out := MapRowKV(kvs[:0], m, 5); len(out) != 0 {
		t.Errorf("out-of-range MapRowKV returned %d kvs, want 0", len(out))
	}
}

// TestColStrRowVariants ensures the shared string reader handles all three column
// families without panicking.
func TestColStrRowVariants(t *testing.T) {
	plain := newColString(0, 0)
	plain.Append("hello")
	if got := ColStrRow(&plain, 0); got != "hello" {
		t.Errorf("ColStr: got %q", got)
	}

	std := proto.NewLowCardinality[string](new(proto.ColStr))
	std.Append("world")
	if got := ColStrRow(std, 0); got != "world" {
		t.Errorf("ColLowCardinality: got %q", got)
	}

	if got := ColStrRow(&plain, 99); got != "" {
		t.Errorf("out-of-range should be empty, got %q", got)
	}
}
