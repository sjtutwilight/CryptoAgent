package role

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
)

const (
	defaultStartupGrace = 300 * time.Millisecond
	defaultStopTimeout  = 5 * time.Second
)

// RunnableRole 抽象角色运行时，便于生命周期管理与测试注入。
type RunnableRole interface {
	Start(ctx context.Context) error
	RoleID() string
}

type roleBuilder func(config.RoleConfig) (RunnableRole, error)

type ManagerOption func(*Manager)

func WithRoleBuilder(builder func(config.RoleConfig) (RunnableRole, error)) ManagerOption {
	return func(m *Manager) {
		if builder != nil {
			m.buildRole = builder
		}
	}
}

func WithStartupGrace(timeout time.Duration) ManagerOption {
	return func(m *Manager) {
		if timeout > 0 {
			m.startupGrace = timeout
		}
	}
}

func WithStopTimeout(timeout time.Duration) ManagerOption {
	return func(m *Manager) {
		if timeout > 0 {
			m.stopTimeout = timeout
		}
	}
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	applyMu sync.Mutex

	mu    sync.Mutex
	roles map[string]*roleRunner

	buildRole    roleBuilder
	startupGrace time.Duration
	stopTimeout  time.Duration

	dataSources       []config.DataSourceConfig
	rateLimitProfiles config.RateLimitProfiles
	roleTemplates     []config.RoleTemplateConfig
	pipelineTemplates []config.PipelineConfig
}

type roleRunner struct {
	role   RunnableRole
	config config.RoleConfig

	cancel context.CancelFunc
	done   chan struct{}
	exitCh chan error
}

type ApplyRoleResult struct {
	RoleID string `json:"role_id"`
	Action string `json:"action"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type ApplyResult struct {
	Status  string            `json:"status"`
	Results []ApplyRoleResult `json:"results"`
}

type applyPlan struct {
	resolvedConfigs map[string]config.RoleConfig
	adds            []config.RoleConfig
	updates         []config.RoleConfig
	removes         []string
	unchanged       []string
}

type roleApplyError struct {
	RoleID string
	Action string
	Err    error
}

func (e *roleApplyError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s role %s: %v", e.Action, e.RoleID, e.Err)
}

func (e *roleApplyError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewManager(parent context.Context, dataSources []config.DataSourceConfig, rateLimitProfiles config.RateLimitProfiles, roleTemplates []config.RoleTemplateConfig, pipelineTemplates []config.PipelineConfig) *Manager {
	return NewManagerWithOptions(parent, dataSources, rateLimitProfiles, roleTemplates, pipelineTemplates)
}

func NewManagerWithOptions(parent context.Context, dataSources []config.DataSourceConfig, rateLimitProfiles config.RateLimitProfiles, roleTemplates []config.RoleTemplateConfig, pipelineTemplates []config.PipelineConfig, opts ...ManagerOption) *Manager {
	ctx, cancel := context.WithCancel(parent)
	m := &Manager{
		ctx:               ctx,
		cancel:            cancel,
		roles:             make(map[string]*roleRunner),
		buildRole:         defaultRoleBuilder,
		startupGrace:      defaultStartupGrace,
		stopTimeout:       defaultStopTimeout,
		dataSources:       dataSources,
		rateLimitProfiles: rateLimitProfiles,
		roleTemplates:     roleTemplates,
		pipelineTemplates: pipelineTemplates,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}

func defaultRoleBuilder(rc config.RoleConfig) (RunnableRole, error) {
	return Build(rc)
}

// Apply 兼容旧调用：仅当全部角色成功切换时返回 nil。
func (m *Manager) Apply(configs []config.RoleConfig) error {
	res, err := m.ApplyWithResult(configs)
	if err != nil {
		return err
	}
	if res.Status != "ok" {
		return fmt.Errorf("apply finished with status %s", res.Status)
	}
	return nil
}

func (m *Manager) ApplyWithResult(configs []config.RoleConfig) (ApplyResult, error) {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	resolved, err := m.resolveConfigs(configs)
	if err != nil {
		return ApplyResult{Status: "failed"}, err
	}

	plan := m.buildPlan(resolved)
	if err := m.prepare(plan); err != nil {
		result := ApplyResult{Status: "failed"}
		if applyErr, ok := err.(*roleApplyError); ok {
			result.Results = append(result.Results, ApplyRoleResult{
				RoleID: applyErr.RoleID,
				Action: applyErr.Action,
				Status: "failed",
				Error:  applyErr.Err.Error(),
			})
		}
		return result, err
	}

	results := m.commit(plan)
	return ApplyResult{
		Status:  summarizeApplyStatus(results),
		Results: results,
	}, nil
}

func (m *Manager) resolveConfigs(configs []config.RoleConfig) ([]config.RoleConfig, error) {
	resolved := make([]config.RoleConfig, len(configs))
	copy(resolved, configs)

	if err := config.ApplyRoleTemplates(resolved, m.roleTemplates); err != nil {
		return nil, err
	}
	if err := config.ApplyPipelineTemplates(resolved, m.pipelineTemplates); err != nil {
		return nil, err
	}
	if err := config.ApplyDataSourcesToRoles(resolved, m.dataSources, m.rateLimitProfiles); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(resolved))
	for i := range resolved {
		if err := resolved[i].Validate(); err != nil {
			return nil, fmt.Errorf("role[%d] %w", i, err)
		}
		id := resolved[i].RoleID
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("duplicate role_id: %s", id)
		}
		seen[id] = struct{}{}
	}
	return resolved, nil
}

func (m *Manager) buildPlan(resolved []config.RoleConfig) applyPlan {
	desired := make(map[string]config.RoleConfig, len(resolved))
	desiredOrder := make([]string, 0, len(resolved))
	for _, rc := range resolved {
		desired[rc.RoleID] = rc
		desiredOrder = append(desiredOrder, rc.RoleID)
	}
	sort.Strings(desiredOrder)

	current := m.snapshotCurrentConfigs()
	plan := applyPlan{resolvedConfigs: desired}
	for _, roleID := range desiredOrder {
		rc := desired[roleID]
		currentCfg, exists := current[roleID]
		if !exists {
			plan.adds = append(plan.adds, rc)
			continue
		}
		if reflect.DeepEqual(currentCfg, rc) {
			plan.unchanged = append(plan.unchanged, roleID)
			continue
		}
		plan.updates = append(plan.updates, rc)
	}

	removeIDs := make([]string, 0)
	for roleID := range current {
		if _, ok := desired[roleID]; !ok {
			removeIDs = append(removeIDs, roleID)
		}
	}
	sort.Strings(removeIDs)
	plan.removes = removeIDs
	return plan
}

func (m *Manager) prepare(plan applyPlan) error {
	for _, rc := range plan.adds {
		if err := m.preflightRole(rc); err != nil {
			return &roleApplyError{RoleID: rc.RoleID, Action: "add", Err: err}
		}
	}
	for _, rc := range plan.updates {
		if err := m.preflightRole(rc); err != nil {
			return &roleApplyError{RoleID: rc.RoleID, Action: "update", Err: err}
		}
	}
	return nil
}

func (m *Manager) preflightRole(rc config.RoleConfig) error {
	r, err := m.buildRole(rc)
	if err != nil {
		return fmt.Errorf("build failed: %w", err)
	}
	runner, err := m.startRunner(r, rc)
	if err != nil {
		return fmt.Errorf("startup check failed: %w", err)
	}
	if err := m.stopRunner(runner); err != nil {
		return fmt.Errorf("shutdown after startup check failed: %w", err)
	}
	return nil
}

func (m *Manager) commit(plan applyPlan) []ApplyRoleResult {
	results := make([]ApplyRoleResult, 0, len(plan.adds)+len(plan.updates)+len(plan.removes)+len(plan.unchanged))

	for _, roleID := range plan.unchanged {
		results = append(results, ApplyRoleResult{
			RoleID: roleID,
			Action: "unchanged",
			Status: "unchanged",
		})
	}

	for _, roleID := range plan.removes {
		runner := m.detachRunner(roleID)
		if runner == nil {
			results = append(results, ApplyRoleResult{
				RoleID: roleID,
				Action: "remove",
				Status: "success",
			})
			continue
		}
		log.Printf("stopping role %s", roleID)
		if err := m.stopRunner(runner); err != nil {
			results = append(results, ApplyRoleResult{
				RoleID: roleID,
				Action: "remove",
				Status: "failed",
				Error:  err.Error(),
			})
			continue
		}
		results = append(results, ApplyRoleResult{
			RoleID: roleID,
			Action: "remove",
			Status: "success",
		})
	}

	for _, rc := range plan.adds {
		results = append(results, m.applyOne(rc, "add"))
	}
	for _, rc := range plan.updates {
		results = append(results, m.applyOne(rc, "update"))
	}

	return results
}

func (m *Manager) applyOne(rc config.RoleConfig, action string) ApplyRoleResult {
	result := ApplyRoleResult{RoleID: rc.RoleID, Action: action}
	newRole, err := m.buildRole(rc)
	if err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("build failed: %v", err)
		return result
	}

	newRunner, err := m.startRunner(newRole, rc)
	if err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("start failed: %v", err)
		return result
	}

	if action == "add" {
		m.setRunner(rc.RoleID, newRunner)
		result.Status = "success"
		return result
	}

	oldRunner := m.getRunner(rc.RoleID)
	if oldRunner == nil {
		m.setRunner(rc.RoleID, newRunner)
		result.Status = "success"
		return result
	}

	m.setRunner(rc.RoleID, newRunner)
	if err := m.stopRunner(oldRunner); err != nil {
		// 回滚当前角色：恢复旧 runner，并关闭新 runner。
		m.setRunner(rc.RoleID, oldRunner)
		if stopNewErr := m.stopRunner(newRunner); stopNewErr != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("rollback failed after stop old error: %v; stop new error: %v", err, stopNewErr)
			return result
		}
		result.Status = "rolled_back"
		result.Error = fmt.Sprintf("switch failed and rolled back: %v", err)
		return result
	}

	result.Status = "success"
	return result
}

func summarizeApplyStatus(results []ApplyRoleResult) string {
	if len(results) == 0 {
		return "ok"
	}
	hasFailure := false
	hasSuccess := false
	for _, r := range results {
		switch strings.ToLower(r.Status) {
		case "failed", "rolled_back":
			hasFailure = true
		case "success", "unchanged":
			hasSuccess = true
		}
	}
	if hasFailure && hasSuccess {
		return "partial_success"
	}
	if hasFailure {
		return "failed"
	}
	return "ok"
}

func (m *Manager) RunningRoles() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.roles))
	for id := range m.roles {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (m *Manager) Shutdown() {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	m.stopAllLocked()
	m.cancel()
}

func (m *Manager) Stop(roleIDs []string) []string {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	if len(roleIDs) == 0 {
		return m.stopAllLocked()
	}

	runners := make([]*roleRunner, 0, len(roleIDs))
	stopped := make([]string, 0, len(roleIDs))

	m.mu.Lock()
	for _, id := range roleIDs {
		if runner, ok := m.roles[id]; ok {
			runners = append(runners, runner)
			stopped = append(stopped, id)
			delete(m.roles, id)
			log.Printf("stopping role %s", id)
		}
	}
	m.mu.Unlock()

	for _, runner := range runners {
		if err := m.stopRunner(runner); err != nil {
			log.Printf("stop role %s timeout: %v", runner.role.RoleID(), err)
		}
	}

	sort.Strings(stopped)
	return stopped
}

func (m *Manager) stopAllLocked() []string {
	m.mu.Lock()
	runners := make([]*roleRunner, 0, len(m.roles))
	stopped := make([]string, 0, len(m.roles))
	for id, runner := range m.roles {
		runners = append(runners, runner)
		stopped = append(stopped, id)
		delete(m.roles, id)
		log.Printf("stopping role %s", id)
	}
	m.mu.Unlock()

	for _, runner := range runners {
		if err := m.stopRunner(runner); err != nil {
			log.Printf("stop role %s timeout: %v", runner.role.RoleID(), err)
		}
	}

	sort.Strings(stopped)
	return stopped
}

func (m *Manager) snapshotCurrentConfigs() map[string]config.RoleConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]config.RoleConfig, len(m.roles))
	for id, runner := range m.roles {
		cfg := runner.config
		if cfg.RoleID == "" {
			cfg.RoleID = id
		}
		out[id] = cfg
	}
	return out
}

func (m *Manager) startRunner(r RunnableRole, rc config.RoleConfig) (*roleRunner, error) {
	ctx, cancel := context.WithCancel(m.ctx)
	runner := &roleRunner{
		role:   r,
		config: rc,
		cancel: cancel,
		done:   make(chan struct{}),
		exitCh: make(chan error, 1),
	}

	go func() {
		defer close(runner.done)
		err := r.Start(ctx)
		runner.exitCh <- err
		if err != nil {
			log.Printf("role %s exited with error: %v", r.RoleID(), err)
		} else {
			log.Printf("role %s exited", r.RoleID())
		}
		m.mu.Lock()
		if current, ok := m.roles[r.RoleID()]; ok && current == runner {
			delete(m.roles, r.RoleID())
		}
		m.mu.Unlock()
	}()

	t := time.NewTimer(m.startupGrace)
	defer t.Stop()

	select {
	case err := <-runner.exitCh:
		if err != nil {
			return nil, fmt.Errorf("role %s exited during startup: %w", r.RoleID(), err)
		}
		return nil, fmt.Errorf("role %s exited during startup", r.RoleID())
	case <-t.C:
		return runner, nil
	case <-m.ctx.Done():
		cancel()
		<-runner.done
		return nil, fmt.Errorf("manager shutting down: %w", m.ctx.Err())
	}
}

func (m *Manager) stopRunner(runner *roleRunner) error {
	if runner == nil {
		return nil
	}
	runner.cancel()
	t := time.NewTimer(m.stopTimeout)
	defer t.Stop()

	select {
	case <-runner.done:
		return nil
	case <-t.C:
		return fmt.Errorf("timeout waiting role %s stop", runner.role.RoleID())
	}
}

func (m *Manager) setRunner(roleID string, runner *roleRunner) {
	m.mu.Lock()
	m.roles[roleID] = runner
	m.mu.Unlock()
}

func (m *Manager) getRunner(roleID string) *roleRunner {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.roles[roleID]
}

func (m *Manager) detachRunner(roleID string) *roleRunner {
	m.mu.Lock()
	defer m.mu.Unlock()
	runner := m.roles[roleID]
	delete(m.roles, roleID)
	return runner
}
