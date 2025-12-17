import React from 'react';
import { Chip } from '@mui/material';
import {
  Info as InfoIcon,
  Warning as WarningIcon,
  Error as CriticalIcon,
} from '@mui/icons-material';
import { AlertLevelConfig } from '../../services/qualityApi';

/**
 * 告警级别徽章组件
 * 根据告警级别显示不同颜色和图标
 */

// 图标映射
const levelIcons = {
  INFO: InfoIcon,
  WARNING: WarningIcon,
  CRITICAL: CriticalIcon,
};

const AlertLevelBadge = ({ level, size = 'small', showIcon = true }) => {
  const config = AlertLevelConfig[level] || AlertLevelConfig.INFO;
  const IconComponent = levelIcons[level] || InfoIcon;

  return (
    <Chip
      label={config.label}
      size={size}
      icon={showIcon ? <IconComponent sx={{ fontSize: 14 }} /> : undefined}
      sx={{
        backgroundColor: config.bgColor,
        color: config.color,
        fontWeight: 500,
        fontSize: '0.75rem',
        height: size === 'small' ? 24 : 28,
        '& .MuiChip-icon': {
          color: config.color,
          marginLeft: '6px',
        },
      }}
    />
  );
};

export default AlertLevelBadge;

