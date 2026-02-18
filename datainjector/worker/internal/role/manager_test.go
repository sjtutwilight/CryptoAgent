package role

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
)

type buildStep struct {
	buildErr error
	startErr error
}

type scriptedBuilder struct {
	mu    sync.Mutex
	steps map[string][]buildStep
}

func newScriptedBuilder() *scriptedBuilder {
	return &scriptedBuilder{steps: map[string][]buildStep{}}
}

func (b *scriptedBuilder) SetSteps(roleID string, steps ...buildStep) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cloned := make([]buildStep, len(steps))
	copy(cloned, steps)
	b.steps[roleID] = cloned
}

func (b *scriptedBuilder) Build(rc config.RoleConfig) (RunnableRole, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	step := buildStep{}
	if seq := b.steps[rc.RoleID]; len(seq) > 0 {
		step = seq[0]
		b.steps[rc.RoleID] = seq[1:]
	}
	if step.buildErr != nil {
		return nil, step.buildErr
	}
	return &stubRole{id: rc.RoleID, startErr: step.startErr}, nil
}

type stubRole struct {
	id       string
	startErr error
}

func (r *stubRole) RoleID() string {
	return r.id
}

func (r *stubRole) Start(ctx context.Context) error {
	if r.startErr != nil {
		return r.startErr
	}
	<-ctx.Done()
	return nil
}

func testRoleConfig(roleID, version string) config.RoleConfig {
	return config.RoleConfig{
		RoleID:      roleID,
		Emitter:     "single",
		Caller:      "sdk_call",
		CallerClass: version,
	}
}

func TestApplyPrepareFailureKeepsRunningRoles(t *testing.T) {
	builder := newScriptedBuilder()
	m := NewManagerWithOptions(
		context.Background(),
		nil,
		nil,
		nil,
		nil,
		WithRoleBuilder(builder.Build),
		WithStartupGrace(20*time.Millisecond),
		WithStopTimeout(100*time.Millisecond),
	)
	defer m.Shutdown()

	if _, err := m.ApplyWithResult([]config.RoleConfig{testRoleConfig("a", "v1")}); err != nil {
		t.Fatalf("seed apply failed: %v", err)
	}

	builder.SetSteps("bad", buildStep{buildErr: errors.New("build failed")})

	res, err := m.ApplyWithResult([]config.RoleConfig{
		testRoleConfig("a", "v1"),
		testRoleConfig("bad", "v1"),
	})
	if err == nil {
		t.Fatalf("expected prepare error, got nil")
	}
	if res.Status != "failed" {
		t.Fatalf("expected status failed, got %s", res.Status)
	}

	running := m.RunningRoles()
	if len(running) != 1 || running[0] != "a" {
		t.Fatalf("expected running role [a], got %v", running)
	}
}

func TestApplyCommitFailureIsolatedPerRole(t *testing.T) {
	builder := newScriptedBuilder()
	m := NewManagerWithOptions(
		context.Background(),
		nil,
		nil,
		nil,
		nil,
		WithRoleBuilder(builder.Build),
		WithStartupGrace(20*time.Millisecond),
		WithStopTimeout(100*time.Millisecond),
	)
	defer m.Shutdown()

	if _, err := m.ApplyWithResult([]config.RoleConfig{
		testRoleConfig("r1", "v1"),
		testRoleConfig("r2", "v1"),
	}); err != nil {
		t.Fatalf("seed apply failed: %v", err)
	}

	builder.SetSteps("r2",
		buildStep{}, // prepare
		buildStep{startErr: errors.New("commit start failed")},
	)

	res, err := m.ApplyWithResult([]config.RoleConfig{
		testRoleConfig("r1", "v2"),
		testRoleConfig("r2", "v2"),
	})
	if err != nil {
		t.Fatalf("apply returned unexpected error: %v", err)
	}
	if res.Status != "partial_success" {
		t.Fatalf("expected partial_success, got %s", res.Status)
	}

	resultByRole := map[string]ApplyRoleResult{}
	for _, r := range res.Results {
		resultByRole[r.RoleID] = r
	}
	if got := resultByRole["r1"].Status; got != "success" {
		t.Fatalf("expected r1 success, got %s", got)
	}
	if got := resultByRole["r2"].Status; got != "failed" {
		t.Fatalf("expected r2 failed, got %s", got)
	}

	running := m.RunningRoles()
	if len(running) != 2 || running[0] != "r1" || running[1] != "r2" {
		t.Fatalf("expected running roles [r1 r2], got %v", running)
	}

	m.mu.Lock()
	r2Cfg := m.roles["r2"].config
	m.mu.Unlock()
	if r2Cfg.CallerClass != "v1" {
		t.Fatalf("expected r2 rolled back to v1, got %s", r2Cfg.CallerClass)
	}
}

func TestApplyRemoveOnlyAffectsTargetRole(t *testing.T) {
	builder := newScriptedBuilder()
	m := NewManagerWithOptions(
		context.Background(),
		nil,
		nil,
		nil,
		nil,
		WithRoleBuilder(builder.Build),
		WithStartupGrace(20*time.Millisecond),
		WithStopTimeout(100*time.Millisecond),
	)
	defer m.Shutdown()

	if _, err := m.ApplyWithResult([]config.RoleConfig{
		testRoleConfig("a", "v1"),
		testRoleConfig("b", "v1"),
		testRoleConfig("c", "v1"),
	}); err != nil {
		t.Fatalf("seed apply failed: %v", err)
	}

	res, err := m.ApplyWithResult([]config.RoleConfig{
		testRoleConfig("a", "v1"),
		testRoleConfig("c", "v1"),
	})
	if err != nil {
		t.Fatalf("apply returned unexpected error: %v", err)
	}
	if res.Status != "ok" {
		t.Fatalf("expected ok, got %s", res.Status)
	}

	foundRemoveB := false
	for _, r := range res.Results {
		if r.RoleID == "b" && r.Action == "remove" && r.Status == "success" {
			foundRemoveB = true
			break
		}
	}
	if !foundRemoveB {
		t.Fatalf("expected remove success result for role b, got %+v", res.Results)
	}

	running := m.RunningRoles()
	sort.Strings(running)
	if len(running) != 2 || running[0] != "a" || running[1] != "c" {
		t.Fatalf("expected running roles [a c], got %v", running)
	}
}
