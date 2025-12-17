import React from 'react';
import { Box, Typography, LinearProgress, Tooltip } from '@mui/material';

/**
 * 质量指标仪表组件
 * 展示完整度、新鲜度、Schema漂移等指标
 */

// 获取指标颜色
const getScoreColor = (score) => {
  if (score >= 0.8) return '#4ade80';  // 绿色 - 优秀
  if (score >= 0.6) return '#fbbf24';  // 黄色 - 良好
  if (score >= 0.4) return '#fb923c';  // 橙色 - 一般
  return '#f87171';                     // 红色 - 差
};

// 获取指标等级
const getScoreLevel = (score) => {
  if (score >= 0.8) return '优秀';
  if (score >= 0.6) return '良好';
  if (score >= 0.4) return '一般';
  return '较差';
};

const QualityGaugeItem = ({ label, value, description }) => {
  const score = value ?? 0;
  const percentage = Math.round(score * 100);
  const color = getScoreColor(score);
  const level = getScoreLevel(score);

  return (
    <Tooltip title={description} placement="top">
      <Box sx={{ mb: 2 }}>
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            mb: 0.75,
          }}
        >
          <Typography
            variant="body2"
            sx={{ fontSize: '0.8rem', color: 'text.secondary' }}
          >
            {label}
          </Typography>
          <Box sx={{ display: 'flex', alignItems: 'baseline', gap: 0.5 }}>
            <Typography
              variant="body2"
              sx={{
                fontSize: '0.9rem',
                fontWeight: 600,
                color: color,
                fontFamily: "'JetBrains Mono', monospace",
              }}
            >
              {percentage}%
            </Typography>
            <Typography
              variant="caption"
              sx={{ fontSize: '0.65rem', color: 'text.disabled' }}
            >
              {level}
            </Typography>
          </Box>
        </Box>
        <LinearProgress
          variant="determinate"
          value={percentage}
          sx={{
            height: 6,
            borderRadius: 3,
            backgroundColor: 'rgba(255, 255, 255, 0.06)',
            '& .MuiLinearProgress-bar': {
              borderRadius: 3,
              backgroundColor: color,
            },
          }}
        />
      </Box>
    </Tooltip>
  );
};

const QualityGauge = ({ quality, compact = false }) => {
  if (!quality) {
    return (
      <Box
        sx={{
          p: 2,
          backgroundColor: 'rgba(255, 255, 255, 0.02)',
          borderRadius: 1.5,
          textAlign: 'center',
        }}
      >
        <Typography variant="body2" color="text.disabled">
          暂无质量数据
        </Typography>
      </Box>
    );
  }

  const { completeness, freshness, schemaDrift, collectedAt } = quality;

  // 计算综合得分
  const overallScore =
    ((completeness ?? 0) + (freshness ?? 0) + (1 - (schemaDrift ?? 0))) / 3;
  const overallColor = getScoreColor(overallScore);
  const overallPercentage = Math.round(overallScore * 100);

  if (compact) {
    return (
      <Tooltip title={`质量评分: ${overallPercentage}%`}>
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: 0.75,
            px: 1.5,
            py: 0.5,
            borderRadius: 1,
            backgroundColor: `${overallColor}15`,
          }}
        >
          <Box
            sx={{
              width: 8,
              height: 8,
              borderRadius: '50%',
              backgroundColor: overallColor,
            }}
          />
          <Typography
            variant="caption"
            sx={{
              fontWeight: 600,
              color: overallColor,
              fontFamily: "'JetBrains Mono', monospace",
            }}
          >
            {overallPercentage}%
          </Typography>
        </Box>
      </Tooltip>
    );
  }

  return (
    <Box
      sx={{
        p: 2.5,
        backgroundColor: 'rgba(255, 255, 255, 0.02)',
        borderRadius: 2,
        border: '1px solid rgba(255, 255, 255, 0.06)',
      }}
    >
      {/* 综合评分 */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          mb: 3,
          pb: 2,
          borderBottom: '1px solid rgba(255, 255, 255, 0.06)',
        }}
      >
        <Typography variant="h6" sx={{ fontSize: '0.95rem', fontWeight: 600 }}>
          质量评估
        </Typography>
        <Box
          sx={{
            display: 'flex',
            alignItems: 'baseline',
            gap: 0.5,
            px: 1.5,
            py: 0.5,
            borderRadius: 1.5,
            backgroundColor: `${overallColor}15`,
          }}
        >
          <Typography
            sx={{
              fontSize: '1.25rem',
              fontWeight: 700,
              color: overallColor,
              fontFamily: "'JetBrains Mono', monospace",
            }}
          >
            {overallPercentage}
          </Typography>
          <Typography
            variant="caption"
            sx={{ color: overallColor, fontWeight: 500 }}
          >
            分
          </Typography>
        </Box>
      </Box>

      {/* 各项指标 */}
      <QualityGaugeItem
        label="完整度"
        value={completeness}
        description="数据字段的填充率，越高表示数据越完整"
      />
      <QualityGaugeItem
        label="新鲜度"
        value={freshness}
        description="数据更新的及时性，越高表示数据越新"
      />
      <QualityGaugeItem
        label="Schema 稳定性"
        value={schemaDrift != null ? 1 - schemaDrift : null}
        description="Schema 变更频率，越高表示结构越稳定"
      />

      {/* 采集时间 */}
      {collectedAt && (
        <Typography
          variant="caption"
          sx={{
            display: 'block',
            textAlign: 'right',
            color: 'text.disabled',
            mt: 2,
            fontSize: '0.7rem',
          }}
        >
          采集于 {new Date(collectedAt).toLocaleString('zh-CN')}
        </Typography>
      )}
    </Box>
  );
};

export default QualityGauge;

