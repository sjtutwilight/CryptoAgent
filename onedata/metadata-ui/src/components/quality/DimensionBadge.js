import React from 'react';
import { Chip, Tooltip } from '@mui/material';
import {
  CheckCircle as CompletenessIcon,
  Schedule as TimelinessIcon,
  GpsFixed as AccuracyIcon,
  CompareArrows as ConsistencyIcon,
  Schema as SchemaIcon,
} from '@mui/icons-material';
import { DimensionLabels } from '../../services/qualityApi';

/**
 * 质量维度徽章组件
 * 展示不同质量维度的标签
 */

// 维度配置
const dimensionConfig = {
  COMPLETENESS: {
    icon: CompletenessIcon,
    color: '#4ade80',
    bgColor: 'rgba(74, 222, 128, 0.12)',
    description: '检测数据字段的完整性，必填字段是否缺失',
  },
  TIMELINESS: {
    icon: TimelinessIcon,
    color: '#60a5fa',
    bgColor: 'rgba(96, 165, 250, 0.12)',
    description: '检测数据时效性，包括延迟和吞吐量',
  },
  ACCURACY: {
    icon: AccuracyIcon,
    color: '#fbbf24',
    bgColor: 'rgba(251, 191, 36, 0.12)',
    description: '检测数据准确性，数值范围是否合理',
  },
  CONSISTENCY: {
    icon: ConsistencyIcon,
    color: '#f472b6',
    bgColor: 'rgba(244, 114, 182, 0.12)',
    description: '检测数据一致性，跨源对比和序列连续性',
  },
  SCHEMA: {
    icon: SchemaIcon,
    color: '#a78bfa',
    bgColor: 'rgba(167, 139, 250, 0.12)',
    description: '检测Schema合规性，字段类型是否变更',
  },
};

const DimensionBadge = ({ dimension, size = 'small', showIcon = true, showTooltip = true }) => {
  const config = dimensionConfig[dimension] || dimensionConfig.COMPLETENESS;
  const label = DimensionLabels[dimension] || dimension;
  const IconComponent = config.icon;

  const badge = (
    <Chip
      label={label}
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

  if (showTooltip) {
    return (
      <Tooltip title={config.description} placement="top">
        {badge}
      </Tooltip>
    );
  }

  return badge;
};

export default DimensionBadge;

