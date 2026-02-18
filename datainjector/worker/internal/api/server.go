package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/role"
)

type Server struct {
	cfg config.APIServerConfig
	mgr *role.Manager
	srv *http.Server

	dataSources   []config.DataSourceConfig
	rateLimit     config.RateLimitProfiles
	roleTemplates []config.RoleTemplateConfig
	pipelineTpls  []config.PipelineConfig
}

func NewServer(cfg config.APIServerConfig, mgr *role.Manager, dataSources []config.DataSourceConfig, rateLimit config.RateLimitProfiles, roleTemplates []config.RoleTemplateConfig, pipelineTpls []config.PipelineConfig) *Server {
	addr := cfg.Addr
	if addr == "" {
		addr = ":8090"
	}
	s := &Server{
		cfg:           cfg,
		mgr:           mgr,
		dataSources:   dataSources,
		rateLimit:     rateLimit,
		roleTemplates: roleTemplates,
		pipelineTpls:  pipelineTpls,
	}
	mux := http.NewServeMux()
	mux.Handle("/api/roles", s.requireAuth(http.HandlerFunc(s.handleRoles)))
	mux.Handle("/api/roles/apply", s.requireAuth(http.HandlerFunc(s.handleApplyRoles)))
	mux.Handle("/api/roles/stop", s.requireAuth(http.HandlerFunc(s.handleStopRoles)))
	mux.Handle("/api/roles/validate", s.requireAuth(http.HandlerFunc(s.handleValidateRoles)))
	mux.Handle("/api/roles/resolve", s.requireAuth(http.HandlerFunc(s.handleResolveRoles)))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	s.srv = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

func (s *Server) Start() {
	go func() {
		log.Printf("control API listening on %s", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("control API stopped: %v", err)
		}
	}()
}

func (s *Server) Stop(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	if s.cfg.Token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-Worker-Token")
		if token == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		if token != s.cfg.Token {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleRoles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := map[string]any{
		"roles": s.mgr.RunningRoles(),
	}
	s.writeJSON(w, resp)
}

type applyRolesRequest struct {
	Roles []config.RoleConfig `json:"roles"`
}

func (s *Server) handleApplyRoles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req applyRolesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	if len(req.Roles) == 0 {
		http.Error(w, "roles payload required", http.StatusBadRequest)
		return
	}
	result, err := s.mgr.ApplyWithResult(req.Roles)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s.writeJSON(w, map[string]any{
			"status":  "failed",
			"error":   err.Error(),
			"results": result.Results,
		})
		return
	}
	s.writeJSON(w, result)
}

type stopRolesRequest struct {
	RoleIDs []string `json:"role_ids"`
}

func (s *Server) handleStopRoles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req stopRolesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	stopped := s.mgr.Stop(req.RoleIDs)
	s.writeJSON(w, map[string]any{
		"status":  "ok",
		"stopped": stopped,
	})
}

type validateRolesRequest struct {
	Roles []config.RoleConfig `json:"roles"`
}

func (s *Server) handleValidateRoles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req validateRolesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	if len(req.Roles) == 0 {
		http.Error(w, "roles payload required", http.StatusBadRequest)
		return
	}
	roles := make([]config.RoleConfig, len(req.Roles))
	copy(roles, req.Roles)

	if err := config.ApplyRoleTemplates(roles, s.roleTemplates); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := config.ApplyPipelineTemplates(roles, s.pipelineTpls); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := config.ApplyDataSourcesToRoles(roles, s.dataSources, s.rateLimit); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	errs := config.ValidateRoles(roles)
	if len(errs) > 0 {
		s.writeJSON(w, map[string]any{
			"status": "invalid",
			"errors": errs,
		})
		return
	}
	s.writeJSON(w, map[string]any{
		"status": "ok",
	})
}

type resolveRolesRequest struct {
	Roles []config.RoleConfig `json:"roles"`
}

func (s *Server) handleResolveRoles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req resolveRolesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	if len(req.Roles) == 0 {
		http.Error(w, "roles payload required", http.StatusBadRequest)
		return
	}

	roles := make([]config.RoleConfig, len(req.Roles))
	copy(roles, req.Roles)

	if err := config.ApplyRoleTemplates(roles, s.roleTemplates); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := config.ApplyPipelineTemplates(roles, s.pipelineTpls); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := config.ApplyDataSourcesToRoles(roles, s.dataSources, s.rateLimit); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	errs := config.ValidateRoles(roles)
	if len(errs) > 0 {
		s.writeJSON(w, map[string]any{
			"status": "invalid",
			"errors": errs,
			"roles":  roles,
		})
		return
	}

	s.writeJSON(w, map[string]any{
		"status": "ok",
		"roles":  roles,
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(payload); err != nil {
		http.Error(w, fmt.Sprintf("encode response: %v", err), http.StatusInternalServerError)
	}
}
