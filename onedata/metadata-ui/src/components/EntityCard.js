import React from 'react';
import {
  Card,
  CardContent,
  Box,
  Typography,
  Chip,
  Tooltip,
  IconButton,
} from '@mui/material';
import {
  ContentCopy as CopyIcon,
  OpenInNew as OpenIcon,
  AccessTime as TimeIcon,
} from '@mui/icons-material';
import { formatDistanceToNow } from 'date-fns';
import { zhCN } from 'date-fns/locale';
import StatusBadge from './StatusBadge';
import DomainIcon from './DomainIcon';
import TypeBadge from './TypeBadge';

/**
 * 元数据实体卡片组件
 * 展示实体摘要信息
 */
const EntityCard = ({ entity, onClick, style }) => {
  const {
    id,
    name,
    type,
    domain,
    platform,
    locator,
    status,
    updatedAt,
    tags = [],
  } = entity;

  // 复制定位符到剪贴板
  const handleCopyLocator = (e) => {
    e.stopPropagation();
    if (locator) {
      navigator.clipboard.writeText(locator);
    }
  };

  // 格式化更新时间
  const formatTime = (timestamp) => {
    if (!timestamp) return '-';
    try {
      return formatDistanceToNow(new Date(timestamp), {
        addSuffix: true,
        locale: zhCN,
      });
    } catch {
      return '-';
    }
  };

  return (
    <Card
      onClick={() => onClick?.(entity)}
      sx={{
        cursor: 'pointer',
        transition: 'all 0.2s ease-in-out',
        '&:hover': {
          transform: 'translateY(-2px)',
          borderColor: 'primary.main',
          boxShadow: '0 8px 24px rgba(100, 255, 218, 0.1)',
        },
        ...style,
      }}
    >
      <CardContent sx={{ p: 2.5, '&:last-child': { pb: 2.5 } }}>
        {/* 头部：域图标 + 名称 + 状态 */}
        <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.5, mb: 2 }}>
          <DomainIcon domain={domain} size="medium" />
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography
              variant="h6"
              sx={{
                fontSize: '0.95rem',
                fontWeight: 600,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: 'text.primary',
                mb: 0.5,
              }}
            >
              {name}
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <TypeBadge type={type} />
              {platform && (
                <Typography
                  variant="caption"
                  sx={{
                    color: 'text.secondary',
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '0.7rem',
                  }}
                >
                  {platform}
                </Typography>
              )}
            </Box>
          </Box>
          <StatusBadge status={status} size="small" showIcon={false} />
        </Box>

        {/* 定位符 */}
        {locator && (
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1,
              mb: 2,
              p: 1,
              backgroundColor: 'rgba(255, 255, 255, 0.03)',
              borderRadius: 1,
              overflow: 'hidden',
            }}
          >
            <Typography
              variant="body2"
              sx={{
                flex: 1,
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: '0.75rem',
                color: 'text.secondary',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {locator}
            </Typography>
            <Tooltip title="复制定位符">
              <IconButton
                size="small"
                onClick={handleCopyLocator}
                sx={{
                  color: 'text.secondary',
                  '&:hover': { color: 'primary.main' },
                }}
              >
                <CopyIcon sx={{ fontSize: 14 }} />
              </IconButton>
            </Tooltip>
          </Box>
        )}

        {/* 标签 */}
        {tags.length > 0 && (
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75, mb: 2 }}>
            {tags.slice(0, 4).map((tag) => (
              <Chip
                key={tag}
                label={tag}
                size="small"
                variant="outlined"
                sx={{
                  height: 20,
                  fontSize: '0.65rem',
                  borderColor: 'rgba(255, 255, 255, 0.15)',
                  color: 'text.secondary',
                }}
              />
            ))}
            {tags.length > 4 && (
              <Chip
                label={`+${tags.length - 4}`}
                size="small"
                sx={{
                  height: 20,
                  fontSize: '0.65rem',
                  backgroundColor: 'rgba(255, 255, 255, 0.05)',
                  color: 'text.secondary',
                }}
              />
            )}
          </Box>
        )}

        {/* 底部：更新时间 */}
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            pt: 1.5,
            borderTop: '1px solid rgba(255, 255, 255, 0.06)',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <TimeIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
            <Typography
              variant="caption"
              sx={{ color: 'text.secondary', fontSize: '0.7rem' }}
            >
              {formatTime(updatedAt)}
            </Typography>
          </Box>
          <Tooltip title="查看详情">
            <OpenIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
          </Tooltip>
        </Box>
      </CardContent>
    </Card>
  );
};

export default EntityCard;

