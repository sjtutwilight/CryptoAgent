package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
	roleruntime "github.com/twilight-labs/dataplatform/datainjector/worker/internal/role"
)

type apiBuildStep struct {
	buildErr error
	startErr error
}

type apiScriptedBuilder struct {
	mu    sync.Mutex
	steps map[string][]apiBuildStep
}

func newAPIScriptedBuilder() *apiScriptedBuilder {
	return &apiScriptedBuilder{steps: map[string][]apiBuildStep{}}
}

func (b *apiScriptedBuilder) SetSteps(roleID string, steps ...apiBuildStep) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cloned := make([]apiBuildStep, len(steps))
	copy(cloned, steps)
	b.steps[roleID] = cloned
}

func (b *apiScriptedBuilder) Build(rc config.RoleConfig) (roleruntime.RunnableRole, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	step := apiBuildStep{}
	if seq := b.steps[rc.RoleID]; len(seq) > 0 {
		step = seq[0]
		b.steps[rc.RoleID] = seq[1:]
	}
	if step.buildErr != nil {
		return nil, step.buildErr
	}
	return &apiStubRole{id: rc.RoleID, startErr: step.startErr}, nil
}

type apiStubRole struct {
	id       string
	startErr error
}

func (r *apiStubRole) RoleID() string {
	return r.id
}

func (r *apiStubRole) Start(ctx context.Context) error {
	if r.startErr != nil {
		return r.startErr
	}
	<-ctx.Done()
	return nil
}

func apiRole(roleID, version string) config.RoleConfig {
	return config.RoleConfig{
		RoleID:      roleID,
		Emitter:     "single",
		Caller:      "sdk_call",
		CallerClass: version,
	}
}

func TestHandleApplyRolesReturnsPerRoleResults(t *testing.T) {
	builder := newAPIScriptedBuilder()
	mgr := roleruntime.NewManagerWithOptions(
		context.Background(),
		nil,
		nil,
		nil,
		nil,
		roleruntime.WithRoleBuilder(builder.Build),
		roleruntime.WithStartupGrace(20*time.Millisecond),
		roleruntime.WithStopTimeout(100*time.Millisecond),
	)
	defer mgr.Shutdown()

	builder.SetSteps("bad",
		apiBuildStep{},
		apiBuildStep{buildErr: errors.New("commit build failed")},
	)

	srv := NewServer(config.APIServerConfig{}, mgr, nil, nil, nil, nil)
	body := `{"roles":[` +
		`{"role_id":"ok","emitter":"single","caller":"sdk_call","caller_class":"v1"},` +
		`{"role_id":"bad","emitter":"single","caller":"sdk_call","caller_class":"v1"}` +
		`]}`

	req := httptest.NewRequest(http.MethodPost, "/api/roles/apply", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d, body=%s", rec.Code, rec.Body.String())
	}

	var payload struct {
		Status  string                        `json:"status"`
		Results []roleruntime.ApplyRoleResult `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Status != "partial_success" {
		t.Fatalf("expected partial_success, got %s", payload.Status)
	}
	if len(payload.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(payload.Results))
	}

	resultByRole := map[string]roleruntime.ApplyRoleResult{}
	for _, r := range payload.Results {
		resultByRole[r.RoleID] = r
	}
	if resultByRole["ok"].Status != "success" {
		t.Fatalf("expected ok role success, got %s", resultByRole["ok"].Status)
	}
	if resultByRole["bad"].Status != "failed" {
		t.Fatalf("expected bad role failed, got %s", resultByRole["bad"].Status)
	}
}

func TestHandleValidateRolesAcceptsAAVEDiffSnapshotRoles(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	workerRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	baseCfgPath := filepath.Join(workerRoot, "configs", "base.yaml")
	rolesPath := filepath.Join(workerRoot, "configs", "aave", "roles_aave_full_stable.json")

	baseCfg, err := config.Load(baseCfgPath)
	if err != nil {
		t.Fatalf("load base config failed: %v", err)
	}
	roles, err := config.LoadRoles(rolesPath)
	if err != nil {
		t.Fatalf("load roles failed: %v", err)
	}
	if len(roles) == 0 {
		t.Fatalf("roles file is empty")
	}

	mgr := roleruntime.NewManagerWithOptions(context.Background(), nil, nil, nil, nil)
	defer mgr.Shutdown()
	srv := NewServer(
		config.APIServerConfig{},
		mgr,
		baseCfg.DataSources,
		baseCfg.RateLimit,
		baseCfg.RoleTemplates,
		baseCfg.Pipelines,
	)

	reqBody, err := json.Marshal(map[string]any{
		"roles": roles,
	})
	if err != nil {
		t.Fatalf("marshal request failed: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/roles/validate", strings.NewReader(string(reqBody)))
	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode validate response failed: %v", err)
	}
	if payload.Status != "ok" {
		t.Fatalf("expected validate status ok, got %s, body=%s", payload.Status, rec.Body.String())
	}
}
