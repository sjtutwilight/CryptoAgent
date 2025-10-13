package fetcher

import (
	"context"
)

// DataFetcher 数据获取器接口
// 用于polling任务的定制化数据获取
type DataFetcher interface {
	// Fetch 获取数据
	Fetch(ctx context.Context, config map[string]interface{}) ([]byte, error)
	
	// Name 返回fetcher名称
	Name() string
}

// FetcherFactory 数据获取器工厂
type FetcherFactory struct {
	fetchers map[string]func(client interface{}) DataFetcher
}

// NewFetcherFactory 创建工厂实例
func NewFetcherFactory() *FetcherFactory {
	ff := &FetcherFactory{
		fetchers: make(map[string]func(client interface{}) DataFetcher),
	}
	
	// 注册内置fetchers
	ff.Register("balance", func(client interface{}) DataFetcher {
		return NewBalanceFetcher(client)
	})
	ff.Register("block", func(client interface{}) DataFetcher {
		return NewBlockFetcher(client)
	})
	
	return ff
}

// Register 注册fetcher创建函数
func (ff *FetcherFactory) Register(name string, creator func(client interface{}) DataFetcher) {
	ff.fetchers[name] = creator
}

// Create 创建fetcher实例
func (ff *FetcherFactory) Create(name string, client interface{}) (DataFetcher, error) {
	creator, exists := ff.fetchers[name]
	if !exists {
		return nil, nil // 返回nil表示使用默认逻辑
	}
	
	return creator(client), nil
}
