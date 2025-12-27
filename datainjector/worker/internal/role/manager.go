package role

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
)

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	roles map[string]*roleRunner

	dataSources       []config.DataSourceConfig
	rateLimitProfiles config.RateLimitProfiles
	roleTemplates     []config.RoleTemplateConfig
	pipelineTemplates []config.PipelineConfig
}

type roleRunner struct {
	role   *Role
	cancel context.CancelFunc
	done   chan struct{}
}

func NewManager(parent context.Context, dataSources []config.DataSourceConfig, rateLimitProfiles config.RateLimitProfiles, roleTemplates []config.RoleTemplateConfig, pipelineTemplates []config.PipelineConfig) *Manager {
	ctx, cancel := context.WithCancel(parent)
	return &Manager{
		ctx:               ctx,
		cancel:            cancel,
		roles:             make(map[string]*roleRunner),
		dataSources:       dataSources,
		rateLimitProfiles: rateLimitProfiles,
		roleTemplates:     roleTemplates,
		pipelineTemplates: pipelineTemplates,
	}
}

func (m *Manager) Apply(configs []config.RoleConfig) error {
	if err := config.ApplyRoleTemplates(configs, m.roleTemplates); err != nil {
		return err
	}
	if err := config.ApplyPipelineTemplates(configs, m.pipelineTemplates); err != nil {
		return err
	}
	if err := config.ApplyDataSourcesToRoles(configs, m.dataSources, m.rateLimitProfiles); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(configs))
	for i := range configs {
		if err := configs[i].Validate(); err != nil {
			return fmt.Errorf("role[%d] %w", i, err)
		}
		id := configs[i].RoleID
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate role_id: %s", id)
		}
		seen[id] = struct{}{}
	}

	m.stopAll()

	for _, rc := range configs {
		if err := m.startRole(rc); err != nil {
			return fmt.Errorf("start role %s: %w", rc.RoleID, err)
		}
	}
	return nil
}

func (m *Manager) RunningRoles() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.roles))
	for id := range m.roles {
		ids = append(ids, id)
	}
	return ids
}

func (m *Manager) Shutdown() {
	m.stopAll()
	m.cancel()
}

func (m *Manager) Stop(roleIDs []string) []string {
	if len(roleIDs) == 0 {
		return m.stopAll()
	}

	m.mu.Lock()
	runners := make([]*roleRunner, 0, len(roleIDs))
	stopped := make([]string, 0, len(roleIDs))
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
		runner.cancel()
		<-runner.done
	}

	return stopped
}

func (m *Manager) stopAll() []string {
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
		runner.cancel()
		<-runner.done
	}

	return stopped
}

func (m *Manager) startRole(rc config.RoleConfig) error {
	r, err := Build(rc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(m.ctx)
	runner := &roleRunner{
		role:   r,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	m.mu.Lock()
	m.roles[r.ID] = runner
	m.mu.Unlock()

	go func() {
		defer close(runner.done)
		if err := r.Start(ctx); err != nil {
			log.Printf("role %s exited with error: %v", r.ID, err)
		} else {
			log.Printf("role %s exited", r.ID)
		}
		m.mu.Lock()
		if current, ok := m.roles[r.ID]; ok && current == runner {
			delete(m.roles, r.ID)
		}
		m.mu.Unlock()
	}()

	return nil
}
