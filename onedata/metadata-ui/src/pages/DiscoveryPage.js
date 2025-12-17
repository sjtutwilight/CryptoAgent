import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Grid,
  Alert,
  Snackbar,
  Badge,
  IconButton,
  Tooltip,
} from '@mui/material';
import {
  Sync as SyncIcon,
  Notifications as NotificationIcon,
} from '@mui/icons-material';
import SearchFilters from '../components/SearchFilters';
import EntityList from '../components/EntityList';
import DomainStatsCard from '../components/DomainStatsCard';
import useMetadataSearch from '../hooks/useMetadataSearch';
import useSSE from '../hooks/useSSE';
import { getAllDomainStats } from '../services/api';

/**
 * 元数据发现页面
 * 提供搜索、过滤、浏览元数据实体的主界面
 */
const DiscoveryPage = () => {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  // 从 URL 参数初始化搜索条件
  const initialParams = {
    keyword: searchParams.get('keyword') || '',
    domain: searchParams.get('domain') || '',
    type: searchParams.get('type') || '',
    platform: searchParams.get('platform') || '',
    status: searchParams.get('status') || '',
  };

  // 搜索状态
  const {
    entities,
    totalElements,
    totalPages,
    loading,
    error,
    params,
    setKeyword,
    setDomain,
    setType,
    setPlatform,
    setStatus,
    setPage,
    setPageSize,
    setSort,
    resetFilters,
    refresh,
  } = useMetadataSearch(initialParams);

  // 域统计数据
  const [domainStats, setDomainStats] = useState([]);
  const [statsLoading, setStatsLoading] = useState(true);

  // SSE 实时更新
  const { connected, updateCount, resetUpdateCount } = useSSE({
    autoConnect: true,
    onUpdate: (data) => {
      // 收到更新时可以刷新列表或显示提示
      console.log('[SSE Update]', data);
    },
  });

  // 更新提示
  const [showUpdateAlert, setShowUpdateAlert] = useState(false);

  // 加载域统计数据
  useEffect(() => {
    const loadStats = async () => {
      setStatsLoading(true);
      try {
        const stats = await getAllDomainStats();
        setDomainStats(stats);
      } catch (err) {
        console.error('Failed to load domain stats:', err);
      } finally {
        setStatsLoading(false);
      }
    };
    loadStats();
  }, []);

  // 同步 URL 参数
  useEffect(() => {
    const newParams = new URLSearchParams();
    if (params.keyword) newParams.set('keyword', params.keyword);
    if (params.domain) newParams.set('domain', params.domain);
    if (params.type) newParams.set('type', params.type);
    if (params.platform) newParams.set('platform', params.platform);
    if (params.status) newParams.set('status', params.status);
    setSearchParams(newParams, { replace: true });
  }, [params.keyword, params.domain, params.type, params.platform, params.status, setSearchParams]);

  // 有新更新时显示提示
  useEffect(() => {
    if (updateCount > 0) {
      setShowUpdateAlert(true);
    }
  }, [updateCount]);

  // 点击实体跳转详情
  const handleEntityClick = useCallback((entity) => {
    navigate(`/entity/${entity.id}`);
  }, [navigate]);

  // 点击域统计卡片过滤
  const handleDomainClick = useCallback((domain) => {
    setDomain(domain);
    window.scrollTo({ top: 400, behavior: 'smooth' });
  }, [setDomain]);

  // 刷新并清除更新计数
  const handleRefresh = useCallback(() => {
    refresh();
    resetUpdateCount();
    setShowUpdateAlert(false);
  }, [refresh, resetUpdateCount]);

  return (
    <Box sx={{ minHeight: '100vh', pb: 4 }}>
      {/* 页面头部 */}
      <Box
        sx={{
          background: 'linear-gradient(180deg, rgba(100, 255, 218, 0.05) 0%, transparent 100%)',
          borderBottom: '1px solid rgba(255, 255, 255, 0.06)',
          py: 4,
          mb: 4,
        }}
      >
        <Container maxWidth="xl">
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
            <Typography
              variant="h1"
              sx={{
                fontSize: { xs: '1.75rem', md: '2rem' },
                fontWeight: 700,
                background: 'linear-gradient(135deg, #64ffda 0%, #60a5fa 100%)',
                backgroundClip: 'text',
                WebkitBackgroundClip: 'text',
                WebkitTextFillColor: 'transparent',
              }}
            >
              元数据发现
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              {/* SSE 连接状态 */}
              <Tooltip title={connected ? '实时更新已连接' : '实时更新已断开'}>
                <Box
                  sx={{
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    backgroundColor: connected ? '#4ade80' : '#f87171',
                    boxShadow: connected
                      ? '0 0 8px rgba(74, 222, 128, 0.5)'
                      : '0 0 8px rgba(248, 113, 113, 0.5)',
                  }}
                />
              </Tooltip>
              {/* 更新通知 */}
              <Tooltip title={updateCount > 0 ? `${updateCount} 条新更新` : '暂无新更新'}>
                <IconButton
                  size="small"
                  onClick={handleRefresh}
                  sx={{ color: updateCount > 0 ? 'primary.main' : 'text.secondary' }}
                >
                  <Badge badgeContent={updateCount} color="primary" max={99}>
                    <NotificationIcon fontSize="small" />
                  </Badge>
                </IconButton>
              </Tooltip>
              {/* 刷新按钮 */}
              <Tooltip title="刷新">
                <IconButton
                  size="small"
                  onClick={handleRefresh}
                  sx={{ color: 'text.secondary' }}
                >
                  <SyncIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
          </Box>
          <Typography variant="body1" color="text.secondary">
            浏览和搜索 DeFi 协议、数据库表、Kafka 主题、Flink 作业等元数据资产
          </Typography>
        </Container>
      </Box>

      <Container maxWidth="xl">
        {/* 域统计概览 */}
        <Box sx={{ mb: 4 }}>
          <Typography
            variant="h5"
            sx={{ mb: 2, fontWeight: 600, color: 'text.primary' }}
          >
            域概览
          </Typography>
          <Grid container spacing={2}>
            {(statsLoading ? Array(6).fill(null) : domainStats).map((stats, index) => (
              <Grid item xs={12} sm={6} md={4} lg={2} key={stats?.domain || index}>
                <DomainStatsCard
                  stats={stats}
                  loading={statsLoading}
                  onClick={handleDomainClick}
                />
              </Grid>
            ))}
          </Grid>
        </Box>

        {/* 搜索和过滤 */}
        <Box sx={{ mb: 3 }}>
          <SearchFilters
            params={params}
            onKeywordChange={setKeyword}
            onDomainChange={setDomain}
            onTypeChange={setType}
            onPlatformChange={setPlatform}
            onStatusChange={setStatus}
            onReset={resetFilters}
          />
        </Box>

        {/* 实体列表 */}
        <EntityList
          entities={entities}
          totalElements={totalElements}
          totalPages={totalPages}
          page={params.page}
          pageSize={params.size}
          loading={loading}
          error={error}
          onPageChange={setPage}
          onPageSizeChange={setPageSize}
          onEntityClick={handleEntityClick}
          sortBy={params.sortBy}
          sortDirection={params.sortDirection}
          onSortChange={setSort}
        />
      </Container>

      {/* 更新提示 */}
      <Snackbar
        open={showUpdateAlert}
        autoHideDuration={6000}
        onClose={() => setShowUpdateAlert(false)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert
          severity="info"
          onClose={() => setShowUpdateAlert(false)}
          action={
            <IconButton size="small" color="inherit" onClick={handleRefresh}>
              <SyncIcon fontSize="small" />
            </IconButton>
          }
          sx={{
            backgroundColor: 'rgba(96, 165, 250, 0.15)',
            border: '1px solid rgba(96, 165, 250, 0.3)',
          }}
        >
          有 {updateCount} 条元数据更新，点击刷新查看
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default DiscoveryPage;

