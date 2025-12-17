import React from 'react';
import { Chip } from '@mui/material';
import {
  CheckCircle as ActiveIcon,
  PauseCircle as InactiveIcon,
  Warning as DeprecatedIcon,
  Error as FailedIcon,
  HelpOutline as UnknownIcon,
} from '@mui/icons-material';

/**
 * 状态徽章组件
 * 根据元数据状态显示不同颜色和图标
 */

// 状态配置映射
const statusConfig = {
  ACTIVE: {
    label: '活跃',
    color: 'success',
    icon: ActiveIcon,
    bgColor: 'rgba(74, 222, 128, 0.12)',
    textColor: '#4ade80',
  },
  INACTIVE: {
    label: '停用',
    color: 'default',
    icon: InactiveIcon,
    bgColor: 'rgba(160, 160, 176, 0.12)',
    textColor: '#a0a0b0',
  },
  DEPRECATED: {
    label: '废弃',
    color: 'warning',
    icon: DeprecatedIcon,
    bgColor: 'rgba(251, 191, 36, 0.12)',
    textColor: '#fbbf24',
  },
  FAILED: {
    label: '异常',
    color: 'error',
    icon: FailedIcon,
    bgColor: 'rgba(248, 113, 113, 0.12)',
    textColor: '#f87171',
  },
  UNKNOWN: {
    label: '未知',
    color: 'default',
    icon: UnknownIcon,
    bgColor: 'rgba(160, 160, 176, 0.08)',
    textColor: '#808090',
  },
};

const StatusBadge = ({ status, size = 'small', showIcon = true }) => {
  const config = statusConfig[status] || statusConfig.UNKNOWN;
  const IconComponent = config.icon;

  return (
    <Chip
      label={config.label}
      size={size}
      icon={showIcon ? <IconComponent sx={{ fontSize: 14 }} /> : undefined}
      sx={{
        backgroundColor: config.bgColor,
        color: config.textColor,
        fontWeight: 500,
        fontSize: '0.75rem',
        height: size === 'small' ? 24 : 28,
        '& .MuiChip-icon': {
          color: config.textColor,
          marginLeft: '6px',
        },
      }}
    />
  );
};

export default StatusBadge;

