import React from 'react';
import { Box, Card, Typography, Chip, Switch } from '@mui/material';
import DimensionBadge from './DimensionBadge';
import { DomainLabels } from '../../services/qualityApi';

/**
 * 规则卡片组件
 * 展示单条质量规则信息
 */
const RuleCard = ({ rule, onClick }) => {
  const {
    name,
    description,
    dimension,
    domains,
    enabled,
    isAggregate,
  } = rule;

  return (
    <Card
      onClick={onClick}
      sx={{
        p: 2.5,
        cursor: onClick ? 'pointer' : 'default',
        transition: 'all 0.2s ease-in-out',
        opacity: enabled ? 1 : 0.6,
        '&:hover': onClick ? {
          borderColor: 'rgba(100, 255, 218, 0.2)',
          transform: 'translateY(-2px)',
        } : {},
      }}
    >
      {/* 头部：名称和状态 */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'flex-start',
          justifyContent: 'space-between',
          mb: 1.5,
        }}
      >
        <Typography
          variant="body1"
          sx={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: '0.9rem',
            fontWeight: 600,
            color: 'text.primary',
            flex: 1,
            wordBreak: 'break-all',
          }}
        >
          {name}
        </Typography>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, ml: 2 }}>
          {isAggregate && (
            <Chip
              label="聚合"
              size="small"
              sx={{
                backgroundColor: 'rgba(167, 139, 250, 0.12)',
                color: '#a78bfa',
                fontSize: '0.7rem',
                height: 20,
              }}
            />
          )}
          <Switch
            checked={enabled}
            size="small"
            sx={{
              '& .MuiSwitch-switchBase.Mui-checked': {
                color: '#4ade80',
              },
              '& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track': {
                backgroundColor: '#4ade80',
              },
            }}
            onClick={(e) => e.stopPropagation()}
          />
        </Box>
      </Box>

      {/* 描述 */}
      {description && (
        <Typography
          variant="body2"
          sx={{
            color: 'text.secondary',
            fontSize: '0.85rem',
            mb: 2,
            lineHeight: 1.5,
          }}
        >
          {description}
        </Typography>
      )}

      {/* 维度标签 */}
      <Box sx={{ mb: 2 }}>
        <DimensionBadge dimension={dimension} />
      </Box>

      {/* 适用域 */}
      <Box
        sx={{
          pt: 2,
          borderTop: '1px solid rgba(255, 255, 255, 0.06)',
        }}
      >
        <Typography
          variant="caption"
          sx={{
            color: 'text.disabled',
            fontSize: '0.7rem',
            display: 'block',
            mb: 1,
          }}
        >
          适用域
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
          {(domains || []).map((domain) => (
            <Chip
              key={domain}
              label={DomainLabels[domain] || domain}
              size="small"
              sx={{
                backgroundColor: 'rgba(255, 255, 255, 0.06)',
                color: 'text.secondary',
                fontSize: '0.7rem',
                height: 22,
              }}
            />
          ))}
        </Box>
      </Box>
    </Card>
  );
};

export default RuleCard;

