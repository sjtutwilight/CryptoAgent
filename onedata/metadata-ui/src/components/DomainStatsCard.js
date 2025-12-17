import React from 'react';
import { Card, CardContent, Box, Typography, Skeleton } from '@mui/material';
import DomainIcon, { getDomainColor, getDomainLabel } from './DomainIcon';

/**
 * 域统计卡片组件
 * 展示单个域的实体统计数据
 */
const DomainStatsCard = ({ stats, loading, onClick }) => {
  if (loading) {
    return (
      <Card sx={{ height: '100%' }}>
        <CardContent sx={{ p: 2.5 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2 }}>
            <Skeleton variant="rounded" width={32} height={32} />
            <Skeleton variant="text" width={80} height={24} />
          </Box>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <Skeleton variant="rounded" width={60} height={48} />
            <Skeleton variant="rounded" width={60} height={48} />
            <Skeleton variant="rounded" width={60} height={48} />
          </Box>
        </CardContent>
      </Card>
    );
  }

  if (!stats) return null;

  const { domain, totalEntities, activeEntities, criticalEntities } = stats;
  const color = getDomainColor(domain);
  const label = getDomainLabel(domain);

  return (
    <Card
      onClick={() => onClick?.(domain)}
      sx={{
        height: '100%',
        cursor: 'pointer',
        transition: 'all 0.2s ease-in-out',
        '&:hover': {
          transform: 'translateY(-2px)',
          borderColor: color,
          boxShadow: `0 8px 24px ${color}15`,
        },
      }}
    >
      <CardContent sx={{ p: 2.5, '&:last-child': { pb: 2.5 } }}>
        {/* 头部：域图标和名称 */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2.5 }}>
          <DomainIcon domain={domain} size="medium" />
          <Typography
            variant="h6"
            sx={{
              fontSize: '1rem',
              fontWeight: 600,
              color: 'text.primary',
            }}
          >
            {label}
          </Typography>
        </Box>

        {/* 统计数据 */}
        <Box sx={{ display: 'flex', gap: 2 }}>
          {/* 总数 */}
          <Box sx={{ flex: 1 }}>
            <Typography
              variant="h4"
              sx={{
                fontSize: '1.5rem',
                fontWeight: 700,
                color: color,
                fontFamily: "'JetBrains Mono', monospace",
                lineHeight: 1,
              }}
            >
              {totalEntities}
            </Typography>
            <Typography
              variant="caption"
              sx={{ color: 'text.secondary', fontSize: '0.7rem' }}
            >
              总实体
            </Typography>
          </Box>

          {/* 活跃数 */}
          <Box sx={{ flex: 1 }}>
            <Typography
              variant="h4"
              sx={{
                fontSize: '1.5rem',
                fontWeight: 700,
                color: '#4ade80',
                fontFamily: "'JetBrains Mono', monospace",
                lineHeight: 1,
              }}
            >
              {activeEntities}
            </Typography>
            <Typography
              variant="caption"
              sx={{ color: 'text.secondary', fontSize: '0.7rem' }}
            >
              活跃
            </Typography>
          </Box>

          {/* 异常数 */}
          <Box sx={{ flex: 1 }}>
            <Typography
              variant="h4"
              sx={{
                fontSize: '1.5rem',
                fontWeight: 700,
                color: criticalEntities > 0 ? '#f87171' : 'text.disabled',
                fontFamily: "'JetBrains Mono', monospace",
                lineHeight: 1,
              }}
            >
              {criticalEntities}
            </Typography>
            <Typography
              variant="caption"
              sx={{ color: 'text.secondary', fontSize: '0.7rem' }}
            >
              异常
            </Typography>
          </Box>
        </Box>

        {/* 健康度指示条 */}
        <Box
          sx={{
            mt: 2,
            pt: 2,
            borderTop: '1px solid rgba(255, 255, 255, 0.06)',
          }}
        >
          <Box
            sx={{
              height: 4,
              borderRadius: 2,
              backgroundColor: 'rgba(255, 255, 255, 0.06)',
              overflow: 'hidden',
              display: 'flex',
            }}
          >
            {totalEntities > 0 && (
              <>
                <Box
                  sx={{
                    width: `${(activeEntities / totalEntities) * 100}%`,
                    backgroundColor: '#4ade80',
                    transition: 'width 0.5s ease-out',
                  }}
                />
                <Box
                  sx={{
                    width: `${(criticalEntities / totalEntities) * 100}%`,
                    backgroundColor: '#f87171',
                    transition: 'width 0.5s ease-out',
                  }}
                />
              </>
            )}
          </Box>
        </Box>
      </CardContent>
    </Card>
  );
};

export default DomainStatsCard;

