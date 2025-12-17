import React, { useMemo } from 'react';
import { Box, Typography, Skeleton } from '@mui/material';
import { AlertLevelConfig, DomainLabels } from '../../services/qualityApi';

/**
 * 告警统计图表组件
 * 展示告警分布情况（按级别/域/规则）
 */

// 简单的水平条形图组件
const HorizontalBar = ({ label, value, total, color }) => {
  const percentage = total > 0 ? (value / total) * 100 : 0;

  return (
    <Box sx={{ mb: 1.5 }}>
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          mb: 0.5,
        }}
      >
        <Typography
          variant="body2"
          sx={{
            color: 'text.secondary',
            fontSize: '0.8rem',
            maxWidth: '60%',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {label}
        </Typography>
        <Typography
          variant="body2"
          sx={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: '0.8rem',
            fontWeight: 600,
            color,
          }}
        >
          {value}
        </Typography>
      </Box>
      <Box
        sx={{
          height: 6,
          borderRadius: 3,
          backgroundColor: 'rgba(255, 255, 255, 0.06)',
          overflow: 'hidden',
        }}
      >
        <Box
          sx={{
            height: '100%',
            width: `${percentage}%`,
            backgroundColor: color,
            borderRadius: 3,
            transition: 'width 0.3s ease-in-out',
          }}
        />
      </Box>
    </Box>
  );
};

const AlertStatsChart = ({ stats, type = 'level', loading = false }) => {
  // 根据类型处理数据
  const { data, total, title } = useMemo(() => {
    if (!stats) return { data: [], total: 0, title: '' };

    let items = [];
    let sum = 0;
    let chartTitle = '';

    switch (type) {
      case 'level':
        chartTitle = '按告警级别';
        items = Object.entries(stats.byLevel || {}).map(([key, value]) => ({
          label: AlertLevelConfig[key]?.label || key,
          value,
          color: AlertLevelConfig[key]?.color || '#64ffda',
        }));
        break;
      case 'domain':
        chartTitle = '按业务域';
        items = Object.entries(stats.byDomain || {}).map(([key, value]) => ({
          label: DomainLabels[key] || key,
          value,
          color: '#64ffda',
        }));
        break;
      case 'rule':
        chartTitle = '按规则 (Top 10)';
        items = Object.entries(stats.byRule || {}).map(([key, value]) => ({
          label: key.split('.').pop(), // 只显示规则名最后一部分
          value,
          color: '#60a5fa',
        }));
        break;
      default:
        break;
    }

    // 排序并计算总数
    items.sort((a, b) => b.value - a.value);
    sum = items.reduce((acc, item) => acc + item.value, 0);

    return { data: items, total: sum, title: chartTitle };
  }, [stats, type]);

  if (loading) {
    return (
      <Box>
        <Skeleton variant="text" width="40%" height={24} sx={{ mb: 2 }} />
        {[1, 2, 3].map((i) => (
          <Box key={i} sx={{ mb: 1.5 }}>
            <Skeleton variant="text" width="100%" height={20} />
            <Skeleton variant="rectangular" height={6} sx={{ borderRadius: 3 }} />
          </Box>
        ))}
      </Box>
    );
  }

  if (data.length === 0) {
    return (
      <Box
        sx={{
          p: 3,
          textAlign: 'center',
          backgroundColor: 'rgba(255, 255, 255, 0.02)',
          borderRadius: 2,
        }}
      >
        <Typography variant="body2" color="text.disabled">
          暂无数据
        </Typography>
      </Box>
    );
  }

  return (
    <Box>
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          mb: 2,
        }}
      >
        <Typography
          variant="h6"
          sx={{
            fontSize: '0.9rem',
            fontWeight: 600,
            color: 'text.primary',
          }}
        >
          {title}
        </Typography>
        <Typography
          variant="caption"
          sx={{
            color: 'text.disabled',
            fontFamily: "'JetBrains Mono', monospace",
          }}
        >
          共 {total} 条
        </Typography>
      </Box>
      {data.map((item, index) => (
        <HorizontalBar
          key={index}
          label={item.label}
          value={item.value}
          total={total}
          color={item.color}
        />
      ))}
    </Box>
  );
};

export default AlertStatsChart;

