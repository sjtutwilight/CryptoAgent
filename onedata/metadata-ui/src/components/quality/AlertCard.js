import React from 'react';
import { Box, Card, Typography, Chip } from '@mui/material';
import { formatDistanceToNow } from 'date-fns';
import { zhCN } from 'date-fns/locale';
import AlertLevelBadge from './AlertLevelBadge';
import DimensionBadge from './DimensionBadge';
import { DomainLabels } from '../../services/qualityApi';

/**
 * 告警卡片组件
 * 展示单条告警信息
 */
const AlertCard = ({ alert, onClick, compact = false }) => {
  const {
    alertId,
    level,
    domain,
    streamKey,
    dimension,
    ruleName,
    message,
    metricValue,
    threshold,
    alertTime,
  } = alert;

  // 格式化时间
  const timeAgo = alertTime
    ? formatDistanceToNow(new Date(alertTime), { addSuffix: true, locale: zhCN })
    : '未知';

  // 紧凑模式
  if (compact) {
    return (
      <Box
        onClick={onClick}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          p: 1.5,
          borderRadius: 1.5,
          backgroundColor: 'rgba(255, 255, 255, 0.02)',
          border: '1px solid rgba(255, 255, 255, 0.06)',
          cursor: onClick ? 'pointer' : 'default',
          transition: 'all 0.2s',
          '&:hover': onClick ? {
            backgroundColor: 'rgba(255, 255, 255, 0.04)',
            borderColor: 'rgba(100, 255, 218, 0.2)',
          } : {},
        }}
      >
        <AlertLevelBadge level={level} size="small" />
        <Typography
          variant="body2"
          sx={{
            flex: 1,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            color: 'text.primary',
            fontSize: '0.85rem',
          }}
        >
          {message}
        </Typography>
        <Typography variant="caption" color="text.disabled">
          {timeAgo}
        </Typography>
      </Box>
    );
  }

  return (
    <Card
      onClick={onClick}
      sx={{
        p: 2.5,
        cursor: onClick ? 'pointer' : 'default',
        transition: 'all 0.2s ease-in-out',
        '&:hover': onClick ? {
          borderColor: 'rgba(100, 255, 218, 0.2)',
          transform: 'translateY(-2px)',
        } : {},
      }}
    >
      {/* 头部：级别和时间 */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          mb: 2,
        }}
      >
        <AlertLevelBadge level={level} />
        <Typography variant="caption" color="text.disabled">
          {timeAgo}
        </Typography>
      </Box>

      {/* 告警消息 */}
      <Typography
        variant="body1"
        sx={{
          color: 'text.primary',
          fontWeight: 500,
          mb: 1.5,
          lineHeight: 1.5,
        }}
      >
        {message}
      </Typography>

      {/* 标签区 */}
      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
        <DimensionBadge dimension={dimension} />
        <Chip
          label={DomainLabels[domain] || domain}
          size="small"
          sx={{
            backgroundColor: 'rgba(100, 255, 218, 0.08)',
            color: '#64ffda',
            fontSize: '0.75rem',
            height: 24,
          }}
        />
        {streamKey && (
          <Chip
            label={streamKey}
            size="small"
            sx={{
              backgroundColor: 'rgba(255, 255, 255, 0.06)',
              color: 'text.secondary',
              fontSize: '0.75rem',
              height: 24,
              fontFamily: "'JetBrains Mono', monospace",
            }}
          />
        )}
      </Box>

      {/* 指标详情 */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 3,
          pt: 2,
          borderTop: '1px solid rgba(255, 255, 255, 0.06)',
        }}
      >
        <Box>
          <Typography variant="caption" color="text.disabled">
            规则
          </Typography>
          <Typography
            variant="body2"
            sx={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.8rem',
              color: 'text.secondary',
            }}
          >
            {ruleName}
          </Typography>
        </Box>
        {metricValue != null && (
          <Box>
            <Typography variant="caption" color="text.disabled">
              当前值
            </Typography>
            <Typography
              variant="body2"
              sx={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: '0.8rem',
                color: '#f87171',
              }}
            >
              {typeof metricValue === 'number' ? metricValue.toFixed(2) : metricValue}
            </Typography>
          </Box>
        )}
        {threshold != null && (
          <Box>
            <Typography variant="caption" color="text.disabled">
              阈值
            </Typography>
            <Typography
              variant="body2"
              sx={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: '0.8rem',
                color: 'text.secondary',
              }}
            >
              {typeof threshold === 'number' ? threshold.toFixed(2) : threshold}
            </Typography>
          </Box>
        )}
      </Box>
    </Card>
  );
};

export default AlertCard;

