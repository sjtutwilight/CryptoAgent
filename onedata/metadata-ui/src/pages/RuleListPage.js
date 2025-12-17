import React, { useState, useEffect, useCallback } from 'react';
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
  Chip,
  InputAdornment,
  ToggleButton,
  ToggleButtonGroup,
} from '@mui/material';
import {
  Sync as SyncIcon,
  Search as SearchIcon,
  FilterList as FilterIcon,
  Clear as ClearIcon,
  GridView as GridViewIcon,
  ViewList as ListViewIcon,
} from '@mui/icons-material';
import { RuleCard, DimensionBadge } from '../components/quality';
import {
  getQualityRules,
  QualityDimension,
  DimensionLabels,
} from '../services/qualityApi';

/**
 * 规则列表页面
 * 展示所有质量规则，支持筛选和搜索
 */
const RuleListPage = () => {
  // 状态
  const [rules, setRules] = useState([]);
  const [filteredRules, setFilteredRules] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // 筛选条件
  const [searchKeyword, setSearchKeyword] = useState('');
  const [dimensionFilter, setDimensionFilter] = useState('');
  const [enabledFilter, setEnabledFilter] = useState('all'); // all, enabled, disabled
  const [viewMode, setViewMode] = useState('grid'); // grid, list

  // 加载规则数据
  const loadRules = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getQualityRules();
      setRules(data || []);
    } catch (err) {
      console.error('Failed to load rules:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadRules();
  }, [loadRules]);

  // 应用筛选
  useEffect(() => {
    let result = [...rules];

    // 关键字搜索
    if (searchKeyword) {
      const keyword = searchKeyword.toLowerCase();
      result = result.filter(
        (rule) =>
          rule.name.toLowerCase().includes(keyword) ||
          (rule.description && rule.description.toLowerCase().includes(keyword))
      );
    }

    // 维度筛选
    if (dimensionFilter) {
      result = result.filter((rule) => rule.dimension === dimensionFilter);
    }

    // 启用状态筛选
    if (enabledFilter === 'enabled') {
      result = result.filter((rule) => rule.enabled);
    } else if (enabledFilter === 'disabled') {
      result = result.filter((rule) => !rule.enabled);
    }

    setFilteredRules(result);
  }, [rules, searchKeyword, dimensionFilter, enabledFilter]);

  // 清除筛选
  const handleClearFilters = () => {
    setSearchKeyword('');
    setDimensionFilter('');
    setEnabledFilter('all');
  };

  // 是否有活跃筛选
  const hasActiveFilters = searchKeyword || dimensionFilter || enabledFilter !== 'all';

  // 统计数据
  const enabledCount = rules.filter((r) => r.enabled).length;
  const aggregateCount = rules.filter((r) => r.isAggregate).length;

  return (
    <Box sx={{ minHeight: '100vh', pb: 4 }}>
      {/* 页面头部 */}
      <Box
        sx={{
          background: 'linear-gradient(180deg, rgba(167, 139, 250, 0.05) 0%, transparent 100%)',
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
                background: 'linear-gradient(135deg, #a78bfa 0%, #f472b6 100%)',
                backgroundClip: 'text',
                WebkitBackgroundClip: 'text',
                WebkitTextFillColor: 'transparent',
              }}
            >
              规则管理
            </Typography>
            <Tooltip title="刷新">
              <IconButton
                onClick={loadRules}
                disabled={loading}
                sx={{ color: 'text.secondary' }}
              >
                <SyncIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
          <Typography variant="body1" color="text.secondary">
            管理数据质量检测规则，共 {rules.length} 条规则，{enabledCount} 条启用，{aggregateCount} 条聚合规则
          </Typography>
        </Container>
      </Box>

      <Container maxWidth="xl">
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

            {/* 关键字搜索 */}
            <TextField
              size="small"
              placeholder="搜索规则名称或描述"
              value={searchKeyword}
              onChange={(e) => setSearchKeyword(e.target.value)}
              sx={{ minWidth: 240 }}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon sx={{ fontSize: 18, color: 'text.disabled' }} />
                  </InputAdornment>
                ),
              }}
            />

            {/* 维度筛选 */}
            <TextField
              select
              size="small"
              label="质量维度"
              value={dimensionFilter}
              onChange={(e) => setDimensionFilter(e.target.value)}
              sx={{ minWidth: 140 }}
            >
              <MenuItem value="">全部</MenuItem>
              {Object.entries(QualityDimension).map(([key, value]) => (
                <MenuItem key={key} value={value}>
                  {DimensionLabels[key]}
                </MenuItem>
              ))}
            </TextField>

            {/* 启用状态筛选 */}
            <ToggleButtonGroup
              size="small"
              value={enabledFilter}
              exclusive
              onChange={(_, value) => value && setEnabledFilter(value)}
              sx={{
                '& .MuiToggleButton-root': {
                  px: 2,
                  py: 0.5,
                  fontSize: '0.8rem',
                  textTransform: 'none',
                  borderColor: 'rgba(255, 255, 255, 0.1)',
                  '&.Mui-selected': {
                    backgroundColor: 'rgba(100, 255, 218, 0.15)',
                    color: 'primary.main',
                    borderColor: 'rgba(100, 255, 218, 0.3)',
                  },
                },
              }}
            >
              <ToggleButton value="all">全部</ToggleButton>
              <ToggleButton value="enabled">启用</ToggleButton>
              <ToggleButton value="disabled">禁用</ToggleButton>
            </ToggleButtonGroup>

            {/* 清除筛选 */}
            {hasActiveFilters && (
              <Tooltip title="清除筛选">
                <IconButton onClick={handleClearFilters} size="small">
                  <ClearIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            )}

            <Box sx={{ flex: 1 }} />

            {/* 视图切换 */}
            <ToggleButtonGroup
              size="small"
              value={viewMode}
              exclusive
              onChange={(_, value) => value && setViewMode(value)}
              sx={{
                '& .MuiToggleButton-root': {
                  px: 1,
                  borderColor: 'rgba(255, 255, 255, 0.1)',
                  '&.Mui-selected': {
                    backgroundColor: 'rgba(100, 255, 218, 0.15)',
                    color: 'primary.main',
                  },
                },
              }}
            >
              <ToggleButton value="grid">
                <GridViewIcon fontSize="small" />
              </ToggleButton>
              <ToggleButton value="list">
                <ListViewIcon fontSize="small" />
              </ToggleButton>
            </ToggleButtonGroup>
          </Box>

          {/* 活跃筛选标签 */}
          {hasActiveFilters && (
            <Box sx={{ display: 'flex', gap: 1, mt: 2 }}>
              {searchKeyword && (
                <Chip
                  label={`搜索: ${searchKeyword}`}
                  size="small"
                  onDelete={() => setSearchKeyword('')}
                  sx={{ backgroundColor: 'rgba(100, 255, 218, 0.1)' }}
                />
              )}
              {dimensionFilter && (
                <Chip
                  label={`维度: ${DimensionLabels[dimensionFilter]}`}
                  size="small"
                  onDelete={() => setDimensionFilter('')}
                  sx={{ backgroundColor: 'rgba(100, 255, 218, 0.1)' }}
                />
              )}
              {enabledFilter !== 'all' && (
                <Chip
                  label={`状态: ${enabledFilter === 'enabled' ? '启用' : '禁用'}`}
                  size="small"
                  onDelete={() => setEnabledFilter('all')}
                  sx={{ backgroundColor: 'rgba(100, 255, 218, 0.1)' }}
                />
              )}
            </Box>
          )}
        </Card>

        {/* 维度统计 */}
        <Box sx={{ display: 'flex', gap: 1, mb: 3, flexWrap: 'wrap' }}>
          {Object.entries(QualityDimension).map(([key, value]) => {
            const count = rules.filter((r) => r.dimension === value).length;
            return (
              <Chip
                key={key}
                label={`${DimensionLabels[key]}: ${count}`}
                size="small"
                onClick={() => setDimensionFilter(dimensionFilter === value ? '' : value)}
                sx={{
                  backgroundColor: dimensionFilter === value
                    ? 'rgba(100, 255, 218, 0.2)'
                    : 'rgba(255, 255, 255, 0.06)',
                  color: dimensionFilter === value ? 'primary.main' : 'text.secondary',
                  cursor: 'pointer',
                  '&:hover': {
                    backgroundColor: 'rgba(100, 255, 218, 0.15)',
                  },
                }}
              />
            );
          })}
        </Box>

        {/* 规则列表 */}
        {loading ? (
          <Grid container spacing={2}>
            {Array(6).fill(null).map((_, i) => (
              <Grid item xs={12} sm={6} lg={4} key={i}>
                <Card sx={{ p: 2.5, height: 200 }}>
                  <Box className="skeleton" sx={{ height: 24, width: '80%', mb: 2 }} />
                  <Box className="skeleton" sx={{ height: 20, width: '100%', mb: 1 }} />
                  <Box className="skeleton" sx={{ height: 20, width: '60%', mb: 2 }} />
                  <Box className="skeleton" sx={{ height: 24, width: 80, mb: 2 }} />
                  <Box className="skeleton" sx={{ height: 40, width: '100%' }} />
                </Card>
              </Grid>
            ))}
          </Grid>
        ) : filteredRules.length > 0 ? (
          <Grid container spacing={2}>
            {filteredRules.map((rule) => (
              <Grid
                item
                xs={12}
                sm={viewMode === 'grid' ? 6 : 12}
                lg={viewMode === 'grid' ? 4 : 12}
                key={rule.name}
              >
                <RuleCard rule={rule} />
              </Grid>
            ))}
          </Grid>
        ) : (
          <Card
            sx={{
              p: 6,
              textAlign: 'center',
              backgroundColor: 'rgba(255, 255, 255, 0.02)',
            }}
          >
            <Typography variant="h6" color="text.secondary" sx={{ mb: 1 }}>
              没有找到规则
            </Typography>
            <Typography variant="body2" color="text.disabled">
              {hasActiveFilters
                ? '没有符合筛选条件的规则，尝试调整筛选条件'
                : '暂无配置的质量规则'}
            </Typography>
          </Card>
        )}
      </Container>
    </Box>
  );
};

export default RuleListPage;

