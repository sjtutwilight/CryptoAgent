import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Grid,
  Card,
  TextField,
  MenuItem,
  IconButton,
  Tooltip,
  Pagination,
  Alert,
  Chip,
  InputAdornment,
} from '@mui/material';
import {
  Sync as SyncIcon,
  Search as SearchIcon,
  FilterList as FilterIcon,
  Clear as ClearIcon,
} from '@mui/icons-material';
import { AlertCard } from '../components/quality';
import {
  getAlerts,
  AlertLevel,
  DataDomain,
  DomainLabels,
  AlertLevelConfig,
} from '../services/qualityApi';

/**
 * 告警列表页面
 * 支持筛选、搜索和分页
 */
const AlertListPage = () => {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  // 从URL参数初始化筛选条件
  const initialFilters = {
    domain: searchParams.get('domain') || '',
    level: searchParams.get('level') || '',
    ruleName: searchParams.get('ruleName') || '',
  };

  // 状态
  const [alerts, setAlerts] = useState([]);
  const [totalElements, setTotalElements] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [page, setPage] = useState(0);
  const [pageSize] = useState(12);
  const [filters, setFilters] = useState(initialFilters);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // 加载告警数据
  const loadAlerts = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getAlerts({
        ...filters,
        page,
        size: pageSize,
      });
      setAlerts(data.content || []);
      setTotalElements(data.totalElements || 0);
      setTotalPages(data.totalPages || 0);
    } catch (err) {
      console.error('Failed to load alerts:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [filters, page, pageSize]);

  useEffect(() => {
    loadAlerts();
  }, [loadAlerts]);

  // 同步URL参数
  useEffect(() => {
    const params = new URLSearchParams();
    if (filters.domain) params.set('domain', filters.domain);
    if (filters.level) params.set('level', filters.level);
    if (filters.ruleName) params.set('ruleName', filters.ruleName);
    setSearchParams(params, { replace: true });
  }, [filters, setSearchParams]);

  // 更新筛选条件
  const handleFilterChange = (key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
    setPage(0); // 重置页码
  };

  // 清除筛选条件
  const handleClearFilters = () => {
    setFilters({ domain: '', level: '', ruleName: '' });
    setPage(0);
  };

  // 是否有活跃筛选
  const hasActiveFilters = filters.domain || filters.level || filters.ruleName;

  return (
    <Box sx={{ minHeight: '100vh', pb: 4 }}>
      {/* 页面头部 */}
      <Box
        sx={{
          background: 'linear-gradient(180deg, rgba(251, 191, 36, 0.05) 0%, transparent 100%)',
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
                background: 'linear-gradient(135deg, #fbbf24 0%, #f87171 100%)',
                backgroundClip: 'text',
                WebkitBackgroundClip: 'text',
                WebkitTextFillColor: 'transparent',
              }}
            >
              告警中心
            </Typography>
            <Tooltip title="刷新">
              <IconButton
                onClick={loadAlerts}
                disabled={loading}
                sx={{ color: 'text.secondary' }}
              >
                <SyncIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
          <Typography variant="body1" color="text.secondary">
            查看和管理数据质量告警，共 {totalElements} 条记录
          </Typography>
        </Container>
      </Box>

      <Container maxWidth="xl">
        {/* 错误提示 */}
        {error && (
          <Alert
            severity="error"
            sx={{ mb: 3, backgroundColor: 'rgba(248, 113, 113, 0.1)' }}
            onClose={() => setError(null)}
          >
            {error}
          </Alert>
        )}

        {/* 筛选区域 */}
        <Card sx={{ p: 2, mb: 3 }}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 2,
              flexWrap: 'wrap',
            }}
          >
            <FilterIcon sx={{ color: 'text.secondary' }} />

            {/* 告警级别筛选 */}
            <TextField
              select
              size="small"
              label="告警级别"
              value={filters.level}
              onChange={(e) => handleFilterChange('level', e.target.value)}
              sx={{ minWidth: 140 }}
            >
              <MenuItem value="">全部</MenuItem>
              {Object.entries(AlertLevel).map(([key, value]) => (
                <MenuItem key={key} value={value}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box
                      sx={{
                        width: 8,
                        height: 8,
                        borderRadius: '50%',
                        backgroundColor: AlertLevelConfig[key]?.color,
                      }}
                    />
                    {AlertLevelConfig[key]?.label}
                  </Box>
                </MenuItem>
              ))}
            </TextField>

            {/* 业务域筛选 */}
            <TextField
              select
              size="small"
              label="业务域"
              value={filters.domain}
              onChange={(e) => handleFilterChange('domain', e.target.value)}
              sx={{ minWidth: 180 }}
            >
              <MenuItem value="">全部</MenuItem>
              {Object.entries(DataDomain).map(([key, value]) => (
                <MenuItem key={key} value={value}>
                  {DomainLabels[key]}
                </MenuItem>
              ))}
            </TextField>

            {/* 规则名称搜索 */}
            <TextField
              size="small"
              label="规则名称"
              placeholder="输入规则名称搜索"
              value={filters.ruleName}
              onChange={(e) => handleFilterChange('ruleName', e.target.value)}
              sx={{ minWidth: 200 }}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon sx={{ fontSize: 18, color: 'text.disabled' }} />
                  </InputAdornment>
                ),
              }}
            />

            {/* 清除筛选 */}
            {hasActiveFilters && (
              <Tooltip title="清除筛选">
                <IconButton onClick={handleClearFilters} size="small">
                  <ClearIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            )}

            {/* 活跃筛选标签 */}
            <Box sx={{ flex: 1 }} />
            {hasActiveFilters && (
              <Box sx={{ display: 'flex', gap: 1 }}>
                {filters.level && (
                  <Chip
                    label={`级别: ${AlertLevelConfig[filters.level]?.label}`}
                    size="small"
                    onDelete={() => handleFilterChange('level', '')}
                    sx={{ backgroundColor: 'rgba(100, 255, 218, 0.1)' }}
                  />
                )}
                {filters.domain && (
                  <Chip
                    label={`域: ${DomainLabels[filters.domain]}`}
                    size="small"
                    onDelete={() => handleFilterChange('domain', '')}
                    sx={{ backgroundColor: 'rgba(100, 255, 218, 0.1)' }}
                  />
                )}
                {filters.ruleName && (
                  <Chip
                    label={`规则: ${filters.ruleName}`}
                    size="small"
                    onDelete={() => handleFilterChange('ruleName', '')}
                    sx={{ backgroundColor: 'rgba(100, 255, 218, 0.1)' }}
                  />
                )}
              </Box>
            )}
          </Box>
        </Card>

        {/* 告警列表 */}
        {loading ? (
          <Grid container spacing={2}>
            {Array(6).fill(null).map((_, i) => (
              <Grid item xs={12} sm={6} lg={4} key={i}>
                <Card sx={{ p: 2.5, height: 200 }}>
                  <Box className="skeleton" sx={{ height: 24, width: 80, mb: 2 }} />
                  <Box className="skeleton" sx={{ height: 20, width: '100%', mb: 1 }} />
                  <Box className="skeleton" sx={{ height: 20, width: '80%', mb: 2 }} />
                  <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                    <Box className="skeleton" sx={{ height: 24, width: 60 }} />
                    <Box className="skeleton" sx={{ height: 24, width: 80 }} />
                  </Box>
                  <Box className="skeleton" sx={{ height: 40, width: '100%' }} />
                </Card>
              </Grid>
            ))}
          </Grid>
        ) : alerts.length > 0 ? (
          <>
            <Grid container spacing={2}>
              {alerts.map((alert) => (
                <Grid item xs={12} sm={6} lg={4} key={alert.alertId}>
                  <AlertCard
                    alert={alert}
                    onClick={() => navigate(`/quality/alerts/${alert.alertId}`)}
                  />
                </Grid>
              ))}
            </Grid>

            {/* 分页 */}
            {totalPages > 1 && (
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  mt: 4,
                }}
              >
                <Pagination
                  count={totalPages}
                  page={page + 1}
                  onChange={(_, newPage) => setPage(newPage - 1)}
                  color="primary"
                  sx={{
                    '& .MuiPaginationItem-root': {
                      color: 'text.secondary',
                    },
                    '& .Mui-selected': {
                      backgroundColor: 'rgba(100, 255, 218, 0.15) !important',
                      color: 'primary.main',
                    },
                  }}
                />
              </Box>
            )}
          </>
        ) : (
          <Card
            sx={{
              p: 6,
              textAlign: 'center',
              backgroundColor: 'rgba(255, 255, 255, 0.02)',
            }}
          >
            <Typography variant="h6" color="text.secondary" sx={{ mb: 1 }}>
              暂无告警记录
            </Typography>
            <Typography variant="body2" color="text.disabled">
              {hasActiveFilters
                ? '没有符合筛选条件的告警，尝试调整筛选条件'
                : '系统运行正常，没有产生告警'}
            </Typography>
          </Card>
        )}
      </Container>
    </Box>
  );
};

export default AlertListPage;

