package handler

import (
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler/integrity"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type integrityAdapter struct {
	handler *integrity.IntegrityHandler
}

func init() {
	Register("integrity", func(cfg map[string]any) (Handler, error) {
		return newIntegrityHandler(cfg)
	})
	// 为兼容旧配置保留 missing_detector 名称。
	Register("missing_detector", func(cfg map[string]any) (Handler, error) {
		return newIntegrityHandler(cfg)
	})
}

func newIntegrityHandler(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	conf, err := integrity.ParseConfig(cfg)
	if err != nil {
		return nil, err
	}
	h, err := integrity.NewIntegrityHandler(conf)
	if err != nil {
		return nil, err
	}
	return &integrityAdapter{handler: h}, nil
}

func (a *integrityAdapter) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil {
		return nil, nil
	}
	return a.handler.Handle(msg)
}

func (a *integrityAdapter) SetBackfillChannel(ch chan<- types.BackfillCmd) {
	if a == nil || a.handler == nil {
		return
	}
	// diff、snapshot、default 共用同一通道，由 BackfillScheduler 根据 cmd 选择。
	a.handler.SetBackfillTarget("default", ch)
	a.handler.SetBackfillTarget("snapshot", ch)
	a.handler.SetBackfillTarget("diff", ch)
}

func (a *integrityAdapter) OnSnapshotApplied(lastSeq uint64) []*types.Message {
	return a.handler.OnSnapshotApplied(lastSeq)
}
