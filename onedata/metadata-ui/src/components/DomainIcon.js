import React from 'react';
import { Box } from '@mui/material';
import {
  Storage as StorageIcon,
  Stream as StreamIcon,
  Hub as HubIcon,
  TableChart as TableIcon,
  AccountTree as TreeIcon,
  Code as CodeIcon,
} from '@mui/icons-material';

/**
 * 域图标组件
 * 根据元数据域显示对应图标和颜色
 */

// 域配置映射
const domainConfig = {
  defi: {
    label: 'DeFi',
    icon: CodeIcon,
    color: '#f472b6',      // 粉色
    bgColor: 'rgba(244, 114, 182, 0.12)',
  },
  clickhouse: {
    label: 'ClickHouse',
    icon: StorageIcon,
    color: '#fbbf24',      // 黄色
    bgColor: 'rgba(251, 191, 36, 0.12)',
  },
  flink: {
    label: 'Flink',
    icon: StreamIcon,
    color: '#60a5fa',      // 蓝色
    bgColor: 'rgba(96, 165, 250, 0.12)',
  },
  kafka: {
    label: 'Kafka',
    icon: HubIcon,
    color: '#4ade80',      // 绿色
    bgColor: 'rgba(74, 222, 128, 0.12)',
  },
  paimon: {
    label: 'Paimon',
    icon: TableIcon,
    color: '#a78bfa',      // 紫色
    bgColor: 'rgba(167, 139, 250, 0.12)',
  },
  postgres: {
    label: 'PostgreSQL',
    icon: StorageIcon,
    color: '#64ffda',      // 青色
    bgColor: 'rgba(100, 255, 218, 0.12)',
  },
};

const DomainIcon = ({ 
  domain, 
  size = 'medium',
  showLabel = false,
  variant = 'icon' // 'icon' | 'badge'
}) => {
  const config = domainConfig[domain?.toLowerCase()] || {
    label: domain || 'Unknown',
    icon: TreeIcon,
    color: '#a0a0b0',
    bgColor: 'rgba(160, 160, 176, 0.12)',
  };
  
  const IconComponent = config.icon;
  const iconSize = size === 'small' ? 16 : size === 'large' ? 28 : 20;

  if (variant === 'badge') {
    return (
      <Box
        sx={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 0.75,
          px: 1.5,
          py: 0.5,
          borderRadius: 1.5,
          backgroundColor: config.bgColor,
          border: `1px solid ${config.color}30`,
        }}
      >
        <IconComponent sx={{ fontSize: iconSize, color: config.color }} />
        {showLabel && (
          <Box
            component="span"
            sx={{
              fontSize: '0.75rem',
              fontWeight: 500,
              color: config.color,
            }}
          >
            {config.label}
          </Box>
        )}
      </Box>
    );
  }

  return (
    <Box
      sx={{
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        width: iconSize + 12,
        height: iconSize + 12,
        borderRadius: 1.5,
        backgroundColor: config.bgColor,
      }}
    >
      <IconComponent sx={{ fontSize: iconSize, color: config.color }} />
    </Box>
  );
};

export const getDomainColor = (domain) => {
  const config = domainConfig[domain?.toLowerCase()];
  return config?.color || '#a0a0b0';
};

export const getDomainLabel = (domain) => {
  const config = domainConfig[domain?.toLowerCase()];
  return config?.label || domain || 'Unknown';
};

export default DomainIcon;

