import React from 'react';
import { Chip } from '@mui/material';

/**
 * 类型徽章组件
 * 显示元数据实体类型
 */

// 类型配置映射
const typeConfig = {
  TABLE: {
    label: '表',
    color: '#60a5fa',
    bgColor: 'rgba(96, 165, 250, 0.12)',
  },
  COLUMN: {
    label: '列',
    color: '#a0a0b0',
    bgColor: 'rgba(160, 160, 176, 0.12)',
  },
  TOPIC: {
    label: '主题',
    color: '#4ade80',
    bgColor: 'rgba(74, 222, 128, 0.12)',
  },
  JOB: {
    label: '作业',
    color: '#fbbf24',
    bgColor: 'rgba(251, 191, 36, 0.12)',
  },
  CONTRACT: {
    label: '合约',
    color: '#f472b6',
    bgColor: 'rgba(244, 114, 182, 0.12)',
  },
  POOL: {
    label: '池',
    color: '#a78bfa',
    bgColor: 'rgba(167, 139, 250, 0.12)',
  },
  DATABASE: {
    label: '数据库',
    color: '#64ffda',
    bgColor: 'rgba(100, 255, 218, 0.12)',
  },
};

const TypeBadge = ({ type, size = 'small' }) => {
  const config = typeConfig[type] || {
    label: type || 'Unknown',
    color: '#a0a0b0',
    bgColor: 'rgba(160, 160, 176, 0.12)',
  };

  return (
    <Chip
      label={config.label}
      size={size}
      sx={{
        backgroundColor: config.bgColor,
        color: config.color,
        fontWeight: 500,
        fontSize: '0.7rem',
        height: size === 'small' ? 22 : 26,
        fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '0.02em',
      }}
    />
  );
};

export default TypeBadge;

