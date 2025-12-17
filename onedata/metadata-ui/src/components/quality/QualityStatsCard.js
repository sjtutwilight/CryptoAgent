import React from 'react';
import { Box, Card, Typography, Skeleton } from '@mui/material';

/**
 * 质量统计卡片组件
 * 展示单个统计指标
 */
const QualityStatsCard = ({
  title,
  value,
  subtitle,
  icon: Icon,
  color = '#64ffda',
  loading = false,
  onClick,
}) => {
  if (loading) {
    return (
      <Card
        sx={{
          p: 2.5,
          height: '100%',
          backgroundColor: '#16161f',
        }}
      >
        <Skeleton variant="circular" width={40} height={40} sx={{ mb: 2 }} />
        <Skeleton variant="text" width="60%" height={24} />
        <Skeleton variant="text" width="40%" height={40} />
        <Skeleton variant="text" width="80%" height={16} />
      </Card>
    );
  }

  return (
    <Card
      onClick={onClick}
      sx={{
        p: 2.5,
        height: '100%',
        backgroundColor: '#16161f',
        cursor: onClick ? 'pointer' : 'default',
        transition: 'all 0.2s ease-in-out',
        '&:hover': onClick ? {
          borderColor: `${color}40`,
          transform: 'translateY(-2px)',
          boxShadow: `0 8px 24px ${color}15`,
        } : {},
      }}
    >
      {/* 图标 */}
      {Icon && (
        <Box
          sx={{
            width: 40,
            height: 40,
            borderRadius: 2,
            backgroundColor: `${color}15`,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            mb: 2,
          }}
        >
          <Icon sx={{ fontSize: 20, color }} />
        </Box>
      )}

      {/* 标题 */}
      <Typography
        variant="body2"
        sx={{
          color: 'text.secondary',
          fontSize: '0.8rem',
          mb: 0.5,
        }}
      >
        {title}
      </Typography>

      {/* 数值 */}
      <Typography
        variant="h4"
        sx={{
          fontWeight: 700,
          color,
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: '1.75rem',
          lineHeight: 1.2,
          mb: 0.5,
        }}
      >
        {value}
      </Typography>

      {/* 副标题 */}
      {subtitle && (
        <Typography
          variant="caption"
          sx={{
            color: 'text.disabled',
            fontSize: '0.7rem',
          }}
        >
          {subtitle}
        </Typography>
      )}
    </Card>
  );
};

export default QualityStatsCard;

