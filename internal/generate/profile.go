package generate

import (
	"fmt"
	"sort"
)

// Profile is the code-defined replacement for the old YAML profiles.
// It groups every per-column generator for both the logs and traces schemas.
// Fields that don't apply to the active data type are simply ignored.
//
// Build a profile with builder methods, e.g.
//
//	p := NewProfile().
//	    WithServiceName(Pool(V("api-gw", 10), V("db", 5))).
//	    WithBody(Pool(V("HTTP request completed", 30))).
//	    WithResourceAttrs(Map(
//	        K("cloud.provider", 1.0, Const("aws")),
//	        K("process.pid", 0.9, Int(1, 65535)),
//	    ))
//
// or just assign the fields directly. The builder is purely cosmetic — both styles
// produce the same generator objects.
type Profile struct {
	Name string

	ServiceName   Gen
	ScopeName     Gen
	ScopeVersion  Gen
	ResourceAttrs *MapGen
	ScopeAttrs    *MapGen

	// Logs-only
	SeverityText Gen
	Body         Gen
	LogAttrs     *MapGen

	// Traces-only
	SpanName      Gen
	SpanKind      Gen
	StatusCode    Gen
	StatusMessage Gen
	SpanAttrs     *MapGen

	// Profiles-only
	SampleType      Gen
	SampleUnit      Gen
	PeriodType      Gen
	PeriodUnit      Gen
	FunctionName    Gen
	FileName        Gen
	MappingFileName Gen
	ProfileAttrs    *MapGen
	SampleAttrs     *MapGen
}

// NewProfile returns an empty Profile. Use the With* builders or set fields directly.
func NewProfile(name string) *Profile { return &Profile{Name: name} }

func (p *Profile) WithServiceName(g Gen) *Profile       { p.ServiceName = g; return p }
func (p *Profile) WithScopeName(g Gen) *Profile         { p.ScopeName = g; return p }
func (p *Profile) WithScopeVersion(g Gen) *Profile      { p.ScopeVersion = g; return p }
func (p *Profile) WithResourceAttrs(m *MapGen) *Profile { p.ResourceAttrs = m; return p }
func (p *Profile) WithScopeAttrs(m *MapGen) *Profile    { p.ScopeAttrs = m; return p }

func (p *Profile) WithSeverityText(g Gen) *Profile { p.SeverityText = g; return p }
func (p *Profile) WithBody(g Gen) *Profile         { p.Body = g; return p }
func (p *Profile) WithLogAttrs(m *MapGen) *Profile { p.LogAttrs = m; return p }

func (p *Profile) WithSpanName(g Gen) *Profile      { p.SpanName = g; return p }
func (p *Profile) WithSpanKind(g Gen) *Profile      { p.SpanKind = g; return p }
func (p *Profile) WithStatusCode(g Gen) *Profile    { p.StatusCode = g; return p }
func (p *Profile) WithStatusMessage(g Gen) *Profile { p.StatusMessage = g; return p }
func (p *Profile) WithSpanAttrs(m *MapGen) *Profile { p.SpanAttrs = m; return p }

func (p *Profile) WithSampleType(g Gen) *Profile       { p.SampleType = g; return p }
func (p *Profile) WithSampleUnit(g Gen) *Profile       { p.SampleUnit = g; return p }
func (p *Profile) WithPeriodType(g Gen) *Profile       { p.PeriodType = g; return p }
func (p *Profile) WithPeriodUnit(g Gen) *Profile       { p.PeriodUnit = g; return p }
func (p *Profile) WithFunctionName(g Gen) *Profile     { p.FunctionName = g; return p }
func (p *Profile) WithFileName(g Gen) *Profile         { p.FileName = g; return p }
func (p *Profile) WithMappingFileName(g Gen) *Profile  { p.MappingFileName = g; return p }
func (p *Profile) WithProfileAttrs(m *MapGen) *Profile { p.ProfileAttrs = m; return p }
func (p *Profile) WithSampleAttrs(m *MapGen) *Profile  { p.SampleAttrs = m; return p }

// applyDefaults fills any unset generators with safe default constants so the
// fillers never need to nil-check on the hot path.
func (p *Profile) applyDefaults() {
	if p.ServiceName == nil {
		p.ServiceName = Const("unknown")
	}
	if p.ScopeName == nil {
		p.ScopeName = Const("")
	}
	if p.ScopeVersion == nil {
		p.ScopeVersion = Const("")
	}
	if p.ResourceAttrs == nil {
		p.ResourceAttrs = Map()
	}
	if p.ScopeAttrs == nil {
		p.ScopeAttrs = Map()
	}
	if p.SeverityText == nil {
		p.SeverityText = Const("INFO")
	}
	if p.Body == nil {
		p.Body = Const("")
	}
	if p.LogAttrs == nil {
		p.LogAttrs = Map()
	}
	if p.SpanName == nil {
		p.SpanName = Const("unknown")
	}
	if p.SpanKind == nil {
		p.SpanKind = Const("SPAN_KIND_INTERNAL")
	}
	if p.StatusCode == nil {
		p.StatusCode = Const("STATUS_CODE_UNSET")
	}
	if p.StatusMessage == nil {
		p.StatusMessage = Const("")
	}
	if p.SpanAttrs == nil {
		p.SpanAttrs = Map()
	}
	if p.SampleType == nil {
		p.SampleType = Pool("cpu", "samples", "alloc_space", "inuse_space", "alloc_objects", "inuse_objects")
	}
	if p.SampleUnit == nil {
		p.SampleUnit = Pool("nanoseconds", "count", "bytes")
	}
	if p.PeriodType == nil {
		p.PeriodType = Const("cpu")
	}
	if p.PeriodUnit == nil {
		p.PeriodUnit = Const("nanoseconds")
	}
	if p.FunctionName == nil {
		p.FunctionName = Pool("main.main", "runtime.mallocgc", "runtime.gcBgMarkWorker", "runtime.systemstack", "runtime.futex", "net/http.(*conn).serve", "net/http.(*ServeMux).ServeHTTP", "sync.(*Mutex).Lock", "encoding/json.Marshal", "encoding/json.Unmarshal", "database/sql.(*DB).query", "runtime.memmove", "runtime.scanobject", "reflect.Value.Call", "bytes.(*Buffer).Write")
	}
	if p.FileName == nil {
		p.FileName = Pool("/usr/local/go/src/runtime/proc.go", "/usr/local/go/src/runtime/malloc.go", "/usr/local/go/src/net/http/server.go", "/usr/local/go/src/sync/mutex.go", "/usr/local/go/src/encoding/json/encode.go", "/app/main.go", "/app/internal/handler/handler.go", "/app/internal/store/store.go")
	}
	if p.MappingFileName == nil {
		p.MappingFileName = Pool("/app/server", "/usr/lib/x86_64-linux-gnu/libc.so.6", "/usr/local/go/pkg/tool/linux_amd64/link", "[vdso]")
	}
	if p.ProfileAttrs == nil {
		p.ProfileAttrs = Map()
	}
	if p.SampleAttrs == nil {
		p.SampleAttrs = Map()
	}
}

// =============================================================================
// Registry: code-defined profiles registered at init() time.
// =============================================================================

var profileRegistry = map[string]func() *Profile{}

// RegisterProfile makes a profile available under the given name. Call from init().
// Panics on duplicate name (caught at process startup).
func RegisterProfile(name string, build func() *Profile) {
	if _, exists := profileRegistry[name]; exists {
		panic("generate: profile already registered: " + name)
	}
	profileRegistry[name] = build
}

// GetProfile builds and returns the profile registered under name.
// Each call returns a fresh instance so generators are not shared across workers
// (most are stateless after construction, but this keeps things simple).
func GetProfile(name string) (*Profile, error) {
	build, ok := profileRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unknown profile %q (registered: %s)", name, registeredProfileNames())
	}
	p := build()
	p.applyDefaults()
	return p, nil
}

// ListProfiles returns the registered profile names, sorted.
func ListProfiles() []string {
	names := make([]string, 0, len(profileRegistry))
	for n := range profileRegistry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func registeredProfileNames() string {
	names := ListProfiles()
	s := ""
	for i, n := range names {
		if i > 0 {
			s += ", "
		}
		s += n
	}
	return s
}
