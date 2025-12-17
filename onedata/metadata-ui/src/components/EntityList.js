import React from 'react';
import {
  Box,
  Grid,
  Typography,
  Pagination,
  CircularProgress,
  Alert,
  FormControl,
  Select,
  MenuItem,
} from '@mui/material';
import { Inbox as EmptyIcon } from '@mui/icons-material';
import EntityCard from './EntityCard';

/**
 * 元数据实体列表组件
 * 展示搜索结果，支持分页和排序
 */
const EntityList = ({
  entities,
  totalElements,
  totalPages,
  page,
  pageSize,
  loading,
  error,
  onPageChange,
  onPageSizeChange,
  onEntityClick,
  sortBy,
  sortDirection,
  onSortChange,
}) => {
  // 加载状态
  if (loading && entities.length === 0) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          py: 8,
          gap: 2,
        }}
      >
        <CircularProgress size={40} sx={{ color: 'primary.main' }} />
        <Typography variant="body2" color="text.secondary">
          正在加载元数据...
        </Typography>
      </Box>
    );
  }

  // 错误状态
  if (error) {
    return (
      <Alert
        severity="error"
        sx={{
          backgroundColor: 'rgba(248, 113, 113, 0.1)',
          border: '1px solid rgba(248, 113, 113, 0.3)',
        }}
      >
        {error}
      </Alert>
    );
  }

  // 空状态
  if (entities.length === 0) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          py: 8,
          gap: 2,
        }}
      >
        <EmptyIcon sx={{ fontSize: 64, color: 'text.disabled' }} />
        <Typography variant="h6" color="text.secondary">
          暂无数据
        </Typography>
        <Typography variant="body2" color="text.disabled">
          尝试调整搜索条件或过滤器
        </Typography>
      </Box>
    );
  }

  return (
    <Box>
      {/* 结果统计和排序 */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          mb: 2,
        }}
      >
        <Typography variant="body2" color="text.secondary">
          共 <strong style={{ color: '#64ffda' }}>{totalElements}</strong> 条结果
          {loading && (
            <CircularProgress
              size={14}
              sx={{ ml: 1, color: 'primary.main', verticalAlign: 'middle' }}
            />
          )}
        </Typography>

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography variant="caption" color="text.secondary">
            排序：
          </Typography>
          <FormControl size="small" sx={{ minWidth: 120 }}>
            <Select
              value={`${sortBy}-${sortDirection}`}
              onChange={(e) => {
                const [field, dir] = e.target.value.split('-');
                onSortChange?.(field, dir);
              }}
              sx={{ fontSize: '0.8rem' }}
            >
              <MenuItem value="updatedAt-DESC">最近更新</MenuItem>
              <MenuItem value="updatedAt-ASC">最早更新</MenuItem>
              <MenuItem value="name-ASC">名称 A-Z</MenuItem>
              <MenuItem value="name-DESC">名称 Z-A</MenuItem>
            </Select>
          </FormControl>
        </Box>
      </Box>

      {/* 实体卡片网格 */}
      <Grid container spacing={2}>
        {entities.map((entity, index) => (
          <Grid item xs={12} sm={6} lg={4} key={entity.id}>
            <EntityCard
              entity={entity}
              onClick={onEntityClick}
              style={{
                animation: 'fadeIn 0.3s ease-out forwards',
                animationDelay: `${index * 0.05}s`,
                opacity: 0,
              }}
            />
          </Grid>
        ))}
      </Grid>

      {/* 分页 */}
      {totalPages > 1 && (
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            mt: 4,
            pt: 2,
            borderTop: '1px solid rgba(255, 255, 255, 0.06)',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="caption" color="text.secondary">
              每页：
            </Typography>
            <FormControl size="small">
              <Select
                value={pageSize}
                onChange={(e) => onPageSizeChange?.(e.target.value)}
                sx={{ fontSize: '0.8rem', minWidth: 70 }}
              >
                <MenuItem value={10}>10</MenuItem>
                <MenuItem value={20}>20</MenuItem>
                <MenuItem value={50}>50</MenuItem>
                <MenuItem value={100}>100</MenuItem>
              </Select>
            </FormControl>
          </Box>

          <Pagination
            count={totalPages}
            page={page + 1}
            onChange={(_, newPage) => onPageChange?.(newPage - 1)}
            color="primary"
            size="medium"
            showFirstButton
            showLastButton
            sx={{
              '& .MuiPaginationItem-root': {
                color: 'text.secondary',
                '&.Mui-selected': {
                  backgroundColor: 'rgba(100, 255, 218, 0.15)',
                  color: 'primary.main',
                },
              },
            }}
          />
        </Box>
      )}
    </Box>
  );
};

export default EntityList;

