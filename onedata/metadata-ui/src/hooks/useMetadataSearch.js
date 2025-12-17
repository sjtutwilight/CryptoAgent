import { useState, useCallback, useEffect, useRef } from 'react';
import { searchEntities } from '../services/api';

/**
 * 元数据搜索 Hook
 * 管理搜索状态、分页、排序
 */
export const useMetadataSearch = (initialParams = {}) => {
  // 搜索参数
  const [params, setParams] = useState({
    keyword: '',
    domain: '',
    type: '',
    platform: '',
    status: '',
    tags: [],
    page: 0,
    size: 20,
    sortBy: 'updatedAt',
    sortDirection: 'DESC',
    ...initialParams,
  });

  // 搜索结果
  const [data, setData] = useState({
    content: [],
    totalElements: 0,
    totalPages: 0,
  });

  // 加载状态
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // 防抖定时器
  const debounceRef = useRef(null);

  // 执行搜索
  const search = useCallback(async (searchParams) => {
    setLoading(true);
    setError(null);
    try {
      const result = await searchEntities(searchParams);
      setData(result);
    } catch (err) {
      setError(err.message);
      setData({ content: [], totalElements: 0, totalPages: 0 });
    } finally {
      setLoading(false);
    }
  }, []);

  // 防抖搜索
  const debouncedSearch = useCallback((searchParams, delay = 300) => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }
    debounceRef.current = setTimeout(() => {
      search(searchParams);
    }, delay);
  }, [search]);

  // 参数变化时自动搜索
  useEffect(() => {
    debouncedSearch(params);
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, [params, debouncedSearch]);

  // 更新搜索参数
  const updateParams = useCallback((updates) => {
    setParams(prev => ({
      ...prev,
      ...updates,
      // 非分页参数变化时重置到第一页
      page: 'page' in updates ? updates.page : 0,
    }));
  }, []);

  // 更新关键字
  const setKeyword = useCallback((keyword) => {
    updateParams({ keyword });
  }, [updateParams]);

  // 更新域
  const setDomain = useCallback((domain) => {
    updateParams({ domain });
  }, [updateParams]);

  // 更新类型
  const setType = useCallback((type) => {
    updateParams({ type });
  }, [updateParams]);

  // 更新平台
  const setPlatform = useCallback((platform) => {
    updateParams({ platform });
  }, [updateParams]);

  // 更新状态
  const setStatus = useCallback((status) => {
    updateParams({ status });
  }, [updateParams]);

  // 更新标签
  const setTags = useCallback((tags) => {
    updateParams({ tags });
  }, [updateParams]);

  // 翻页
  const setPage = useCallback((page) => {
    setParams(prev => ({ ...prev, page }));
  }, []);

  // 设置每页数量
  const setPageSize = useCallback((size) => {
    setParams(prev => ({ ...prev, size, page: 0 }));
  }, []);

  // 设置排序
  const setSort = useCallback((sortBy, sortDirection = 'DESC') => {
    setParams(prev => ({ ...prev, sortBy, sortDirection, page: 0 }));
  }, []);

  // 重置所有过滤器
  const resetFilters = useCallback(() => {
    setParams({
      keyword: '',
      domain: '',
      type: '',
      platform: '',
      status: '',
      tags: [],
      page: 0,
      size: 20,
      sortBy: 'updatedAt',
      sortDirection: 'DESC',
    });
  }, []);

  // 刷新当前页
  const refresh = useCallback(() => {
    search(params);
  }, [search, params]);

  return {
    // 数据
    entities: data.content,
    totalElements: data.totalElements,
    totalPages: data.totalPages,
    // 状态
    loading,
    error,
    // 当前参数
    params,
    // 操作
    setKeyword,
    setDomain,
    setType,
    setPlatform,
    setStatus,
    setTags,
    setPage,
    setPageSize,
    setSort,
    updateParams,
    resetFilters,
    refresh,
  };
};

export default useMetadataSearch;

