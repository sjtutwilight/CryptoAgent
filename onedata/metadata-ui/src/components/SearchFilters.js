import React from 'react';
import {
  Box,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  InputAdornment,
  IconButton,
  Tooltip,
  Chip,
} from '@mui/material';
import {
  Search as SearchIcon,
  Clear as ClearIcon,
  FilterAlt as FilterIcon,
} from '@mui/icons-material';
import { MetadataStatus, MetadataDomain, MetadataType } from '../services/api';

/**
 * 搜索过滤器组件
 * 提供关键字搜索和多维度过滤
 */
const SearchFilters = ({
  params,
  onKeywordChange,
  onDomainChange,
  onTypeChange,
  onPlatformChange,
  onStatusChange,
  onReset,
}) => {
  // 计算活跃过滤器数量
  const activeFilters = [
    params.domain,
    params.type,
    params.platform,
    params.status,
  ].filter(Boolean).length;

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        gap: 2,
        p: 2.5,
        backgroundColor: 'rgba(255, 255, 255, 0.02)',
        borderRadius: 2,
        border: '1px solid rgba(255, 255, 255, 0.06)',
      }}
    >
      {/* 搜索栏 */}
      <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
        <TextField
          fullWidth
          placeholder="搜索元数据名称、定位符..."
          value={params.keyword}
          onChange={(e) => onKeywordChange(e.target.value)}
          size="small"
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon sx={{ color: 'text.secondary', fontSize: 20 }} />
              </InputAdornment>
            ),
            endAdornment: params.keyword && (
              <InputAdornment position="end">
                <IconButton
                  size="small"
                  onClick={() => onKeywordChange('')}
                  sx={{ color: 'text.secondary' }}
                >
                  <ClearIcon fontSize="small" />
                </IconButton>
              </InputAdornment>
            ),
          }}
          sx={{
            '& .MuiOutlinedInput-root': {
              backgroundColor: 'rgba(255, 255, 255, 0.03)',
            },
          }}
        />
        
        {activeFilters > 0 && (
          <Tooltip title="清除所有过滤器">
            <Chip
              icon={<FilterIcon sx={{ fontSize: 16 }} />}
              label={`${activeFilters} 个过滤器`}
              onDelete={onReset}
              size="small"
              sx={{
                backgroundColor: 'rgba(100, 255, 218, 0.12)',
                color: '#64ffda',
                '& .MuiChip-deleteIcon': {
                  color: '#64ffda',
                  '&:hover': {
                    color: '#9effeb',
                  },
                },
              }}
            />
          </Tooltip>
        )}
      </Box>

      {/* 过滤器行 */}
      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
        {/* 域过滤 */}
        <FormControl size="small" sx={{ minWidth: 140 }}>
          <InputLabel>域</InputLabel>
          <Select
            value={params.domain}
            label="域"
            onChange={(e) => onDomainChange(e.target.value)}
          >
            <MenuItem value="">全部</MenuItem>
            <MenuItem value={MetadataDomain.DEFI}>DeFi</MenuItem>
            <MenuItem value={MetadataDomain.CLICKHOUSE}>ClickHouse</MenuItem>
            <MenuItem value={MetadataDomain.FLINK}>Flink</MenuItem>
            <MenuItem value={MetadataDomain.KAFKA}>Kafka</MenuItem>
            <MenuItem value={MetadataDomain.PAIMON}>Paimon</MenuItem>
            <MenuItem value={MetadataDomain.POSTGRES}>PostgreSQL</MenuItem>
          </Select>
        </FormControl>

        {/* 类型过滤 */}
        <FormControl size="small" sx={{ minWidth: 140 }}>
          <InputLabel>类型</InputLabel>
          <Select
            value={params.type}
            label="类型"
            onChange={(e) => onTypeChange(e.target.value)}
          >
            <MenuItem value="">全部</MenuItem>
            <MenuItem value={MetadataType.TABLE}>表</MenuItem>
            <MenuItem value={MetadataType.TOPIC}>主题</MenuItem>
            <MenuItem value={MetadataType.JOB}>作业</MenuItem>
            <MenuItem value={MetadataType.CONTRACT}>合约</MenuItem>
            <MenuItem value={MetadataType.POOL}>池</MenuItem>
            <MenuItem value={MetadataType.DATABASE}>数据库</MenuItem>
            <MenuItem value={MetadataType.COLUMN}>列</MenuItem>
          </Select>
        </FormControl>

        {/* 平台过滤 */}
        <FormControl size="small" sx={{ minWidth: 140 }}>
          <InputLabel>平台</InputLabel>
          <Select
            value={params.platform}
            label="平台"
            onChange={(e) => onPlatformChange(e.target.value)}
          >
            <MenuItem value="">全部</MenuItem>
            <MenuItem value="clickhouse">ClickHouse</MenuItem>
            <MenuItem value="kafka">Kafka</MenuItem>
            <MenuItem value="flink">Flink</MenuItem>
            <MenuItem value="postgres">PostgreSQL</MenuItem>
            <MenuItem value="paimon">Paimon</MenuItem>
            <MenuItem value="ethereum">Ethereum</MenuItem>
            <MenuItem value="bsc">BSC</MenuItem>
            <MenuItem value="polygon">Polygon</MenuItem>
          </Select>
        </FormControl>

        {/* 状态过滤 */}
        <FormControl size="small" sx={{ minWidth: 140 }}>
          <InputLabel>状态</InputLabel>
          <Select
            value={params.status}
            label="状态"
            onChange={(e) => onStatusChange(e.target.value)}
          >
            <MenuItem value="">全部</MenuItem>
            <MenuItem value={MetadataStatus.ACTIVE}>活跃</MenuItem>
            <MenuItem value={MetadataStatus.INACTIVE}>停用</MenuItem>
            <MenuItem value={MetadataStatus.DEPRECATED}>废弃</MenuItem>
            <MenuItem value={MetadataStatus.FAILED}>异常</MenuItem>
            <MenuItem value={MetadataStatus.UNKNOWN}>未知</MenuItem>
          </Select>
        </FormControl>
      </Box>
    </Box>
  );
};

export default SearchFilters;

