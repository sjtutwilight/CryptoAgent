import React from 'react';
import { Box, Typography, Chip } from '@mui/material';
import {
  Add as CreateIcon,
  Edit as UpdateIcon,
  Delete as DeleteIcon,
  Refresh as RefreshIcon,
  Warning as WarningIcon,
} from '@mui/icons-material';
import { formatDistanceToNow } from 'date-fns';
import { zhCN } from 'date-fns/locale';

/**
 * 事件时间线组件
 * 展示元数据实体的变更历史
 */

// 事件类型配置
const eventTypeConfig = {
  CREATE: {
    label: '创建',
    icon: CreateIcon,
    color: '#4ade80',
    bgColor: 'rgba(74, 222, 128, 0.12)',
  },
  UPDATE: {
    label: '更新',
    icon: UpdateIcon,
    color: '#60a5fa',
    bgColor: 'rgba(96, 165, 250, 0.12)',
  },
  DELETE: {
    label: '删除',
    icon: DeleteIcon,
    color: '#f87171',
    bgColor: 'rgba(248, 113, 113, 0.12)',
  },
  SCHEMA_CHANGE: {
    label: 'Schema变更',
    icon: RefreshIcon,
    color: '#fbbf24',
    bgColor: 'rgba(251, 191, 36, 0.12)',
  },
  QUALITY_ALERT: {
    label: '质量告警',
    icon: WarningIcon,
    color: '#f472b6',
    bgColor: 'rgba(244, 114, 182, 0.12)',
  },
};

// 解析事件字符串（假设格式为 "TYPE: message" 或 "TYPE at timestamp: message"）
const parseEvent = (eventStr) => {
  if (!eventStr) return null;
  
  // 尝试解析 JSON 格式
  try {
    const parsed = JSON.parse(eventStr);
    return {
      type: parsed.type || 'UPDATE',
      message: parsed.message || eventStr,
      timestamp: parsed.timestamp || parsed.occurredAt,
    };
  } catch {
    // 非 JSON，尝试解析简单格式
    const match = eventStr.match(/^(\w+)(?:\s+at\s+(.+?))?:\s*(.*)$/);
    if (match) {
      return {
        type: match[1].toUpperCase(),
        timestamp: match[2],
        message: match[3],
      };
    }
    return {
      type: 'UPDATE',
      message: eventStr,
    };
  }
};

const EventTimelineItem = ({ event, isLast }) => {
  const parsed = parseEvent(event);
  if (!parsed) return null;

  const config = eventTypeConfig[parsed.type] || eventTypeConfig.UPDATE;
  const IconComponent = config.icon;

  // 格式化时间
  const formatTime = (timestamp) => {
    if (!timestamp) return null;
    try {
      return formatDistanceToNow(new Date(timestamp), {
        addSuffix: true,
        locale: zhCN,
      });
    } catch {
      return timestamp;
    }
  };

  return (
    <Box sx={{ display: 'flex', gap: 2, position: 'relative' }}>
      {/* 时间线 */}
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          width: 24,
        }}
      >
        {/* 图标 */}
        <Box
          sx={{
            width: 24,
            height: 24,
            borderRadius: '50%',
            backgroundColor: config.bgColor,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0,
          }}
        >
          <IconComponent sx={{ fontSize: 12, color: config.color }} />
        </Box>
        {/* 连接线 */}
        {!isLast && (
          <Box
            sx={{
              flex: 1,
              width: 2,
              backgroundColor: 'rgba(255, 255, 255, 0.08)',
              my: 0.5,
            }}
          />
        )}
      </Box>

      {/* 内容 */}
      <Box sx={{ flex: 1, pb: isLast ? 0 : 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
          <Chip
            label={config.label}
            size="small"
            sx={{
              height: 18,
              fontSize: '0.65rem',
              backgroundColor: config.bgColor,
              color: config.color,
              fontWeight: 500,
            }}
          />
          {parsed.timestamp && (
            <Typography
              variant="caption"
              sx={{ color: 'text.disabled', fontSize: '0.65rem' }}
            >
              {formatTime(parsed.timestamp)}
            </Typography>
          )}
        </Box>
        <Typography
          variant="body2"
          sx={{
            color: 'text.secondary',
            fontSize: '0.8rem',
            lineHeight: 1.5,
          }}
        >
          {parsed.message}
        </Typography>
      </Box>
    </Box>
  );
};

const EventTimeline = ({ events = [], maxItems = 10 }) => {
  if (events.length === 0) {
    return (
      <Box
        sx={{
          p: 3,
          textAlign: 'center',
          backgroundColor: 'rgba(255, 255, 255, 0.02)',
          borderRadius: 1.5,
        }}
      >
        <Typography variant="body2" color="text.disabled">
          暂无事件记录
        </Typography>
      </Box>
    );
  }

  const displayEvents = events.slice(0, maxItems);

  return (
    <Box
      sx={{
        p: 2.5,
        backgroundColor: 'rgba(255, 255, 255, 0.02)',
        borderRadius: 2,
        border: '1px solid rgba(255, 255, 255, 0.06)',
      }}
    >
      {displayEvents.map((event, index) => (
        <EventTimelineItem
          key={index}
          event={event}
          isLast={index === displayEvents.length - 1}
        />
      ))}
      
      {events.length > maxItems && (
        <Typography
          variant="caption"
          sx={{
            display: 'block',
            textAlign: 'center',
            color: 'text.disabled',
            mt: 2,
            pt: 2,
            borderTop: '1px solid rgba(255, 255, 255, 0.06)',
          }}
        >
          还有 {events.length - maxItems} 条更早的记录
        </Typography>
      )}
    </Box>
  );
};

export default EventTimeline;

