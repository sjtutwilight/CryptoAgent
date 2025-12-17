import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Grid,
  Card,
  IconButton,
  Tooltip,
  Alert,
  Skeleton,
} from '@mui/material';
import {
  Sync as SyncIcon,
  Warning as WarningIcon,
  CheckCircle as HealthyIcon,
  Rule as RuleIcon,
  Notifications as AlertIcon,
  Speed as MetricIcon,
  Window as WindowIcon,
} from '@mui/icons-material';
import {
  QualityStatsCard,
  AlertCard,
  AlertStatsChart,
} from '../components/quality';
import {
  getQualityStatus,
  getQualityHealth,
  getAlertStats,
  getAlerts,
} from '../services/qualityApi';

/**
 * 质量监控仪表盘页面
 * 展示系统状态、告警统计、最近告警等信息
 */
const QualityDashboardPage = () => {
  const navigate = useNavigate();

  // 状态数据
  const [health, setHealth] = useState(null);
  const [status, setStatus] = useState(null);
  const [alertStats, setAlertStats] = useState(null);
  const [recentAlerts, setRecentAlerts] = useState([]);

  // 加载状态
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // 加载数据
  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [healthData, statusData, statsData, alertsData] = await Promise.all([
        getQualityHealth(),
        getQualityStatus(),
        getAlertStats(24),
        getAlerts({ page: 0, size: 5 }),
      ]);
      setHealth(healthData);
      setStatus(statusData);
      setAlertStats(statsData);
      setRecentAlerts(alertsData.content || []);
    } catch (err) {
      console.error('Failed to load quality data:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
    // 每30秒自动刷新
    const interval = setInterval(loadData, 30000);
    return () => clearInterval(interval);
  }, [loadData]);

  // 计算统计数据
  const totalAlerts = alertStats
    ? Object.values(alertStats.byLevel || {}).reduce((a, b) => a + b, 0)
    : 0;
  const criticalAlerts = alertStats?.byLevel?.CRITICAL || 0;
  const warningAlerts = alertStats?.byLevel?.WARNING || 0;

  return (
    <Box sx={{ minHeight: '100vh', pb: 4 }}>
      {/* 页面头部 */}
      <Box
        sx={{
          background: 'linear-gradient(180deg, rgba(100, 255, 218, 0.05) 0%, transparent 100%)',
          borderBottom: '1px solid rgba(255, 255, 255, 0.06)',
          py: 4,
          mb: 4,
        }}
      >
        <Container maxWidth="xl">
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography
                variant="h1"
                sx={{
                  fontSize: { xs: '1.75rem', md: '2rem' },
                  fontWeight: 700,
                  background: 'linear-gradient(135deg, #64ffda 0%, #60a5fa 100%)',
                  backgroundClip: 'text',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent',
                }}
              >
                质量监控
              </Typography>
              {/* 系统状态指示器 */}
              {health && (
                <Tooltip title={health.status === 'UP' ? '系统运行正常' : '系统异常'}>
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 0.5,
                      px: 1.5,
                      py: 0.5,
                      borderRadius: 1,
                      backgroundColor: health.status === 'UP'
                        ? 'rgba(74, 222, 128, 0.12)'
                        : 'rgba(248, 113, 113, 0.12)',
                    }}
                  >
                    {health.status === 'UP' ? (
                      <HealthyIcon sx={{ fontSize: 16, color: '#4ade80' }} />
                    ) : (
                      <WarningIcon sx={{ fontSize: 16, color: '#f87171' }} />
                    )}
                    <Typography
                      variant="caption"
                      sx={{
                        fontWeight: 600,
                        color: health.status === 'UP' ? '#4ade80' : '#f87171',
                      }}
                    >
                      {health.status === 'UP' ? '运行中' : '异常'}
                    </Typography>
                  </Box>
                </Tooltip>
              )}
            </Box>
            <Tooltip title="刷新">
              <IconButton
                onClick={loadData}
                disabled={loading}
                sx={{ color: 'text.secondary' }}
              >
                <SyncIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
          <Typography variant="body1" color="text.secondary">
            实时监控数据质量，查看告警和规则执行状态
          </Typography>
        </Container>
      </Box>

      <Container maxWidth="xl">
        {/* 错误提示 */}
        {error && (
          <Alert
            severity="error"
            sx={{ mb: 3, backgroundColor: 'rgba(248, 113, 113, 0.1)' }}
            onClose={() => setError(null)}
          >
            {error}
          </Alert>
        )}

        {/* 统计卡片 */}
        <Grid container spacing={2} sx={{ mb: 4 }}>
          <Grid item xs={6} sm={4} md={2}>
            <QualityStatsCard
              title="活跃规则"
              value={loading ? '-' : (health?.activeRules || 0)}
              subtitle="当前启用的质量规则"
              icon={RuleIcon}
              color="#64ffda"
              loading={loading}
              onClick={() => navigate('/quality/rules')}
            />
          </Grid>
          <Grid item xs={6} sm={4} md={2}>
            <QualityStatsCard
              title="活跃窗口"
              value={loading ? '-' : (health?.activeWindows || 0)}
              subtitle="聚合规则时间窗口"
              icon={WindowIcon}
              color="#60a5fa"
              loading={loading}
            />
          </Grid>
          <Grid item xs={6} sm={4} md={2}>
            <QualityStatsCard
              title="24h告警"
              value={loading ? '-' : totalAlerts}
              subtitle="过去24小时告警总数"
              icon={AlertIcon}
              color={totalAlerts > 0 ? '#fbbf24' : '#4ade80'}
              loading={loading}
              onClick={() => navigate('/quality/alerts')}
            />
          </Grid>
          <Grid item xs={6} sm={4} md={2}>
            <QualityStatsCard
              title="严重告警"
              value={loading ? '-' : criticalAlerts}
              subtitle="需要立即处理"
              icon={WarningIcon}
              color={criticalAlerts > 0 ? '#f87171' : '#4ade80'}
              loading={loading}
              onClick={() => navigate('/quality/alerts?level=CRITICAL')}
            />
          </Grid>
          <Grid item xs={6} sm={4} md={2}>
            <QualityStatsCard
              title="警告"
              value={loading ? '-' : warningAlerts}
              subtitle="需要关注"
              icon={AlertIcon}
              color={warningAlerts > 0 ? '#fbbf24' : '#4ade80'}
              loading={loading}
              onClick={() => navigate('/quality/alerts?level=WARNING')}
            />
          </Grid>
          <Grid item xs={6} sm={4} md={2}>
            <QualityStatsCard
              title="指标落库"
              value={loading ? '-' : (status?.metrics?.savedCount || 0)}
              subtitle="已保存的质量指标"
              icon={MetricIcon}
              color="#a78bfa"
              loading={loading}
            />
          </Grid>
        </Grid>

        {/* 告警统计和最近告警 */}
        <Grid container spacing={3}>
          {/* 左侧：告警统计图表 */}
          <Grid item xs={12} md={7}>
            <Card sx={{ p: 3, height: '100%' }}>
              <Typography
                variant="h5"
                sx={{ mb: 3, fontWeight: 600, fontSize: '1.1rem' }}
              >
                告警分布 (24h)
              </Typography>
              <Grid container spacing={3}>
                <Grid item xs={12} sm={4}>
                  <AlertStatsChart
                    stats={alertStats}
                    type="level"
                    loading={loading}
                  />
                </Grid>
                <Grid item xs={12} sm={4}>
                  <AlertStatsChart
                    stats={alertStats}
                    type="domain"
                    loading={loading}
                  />
                </Grid>
                <Grid item xs={12} sm={4}>
                  <AlertStatsChart
                    stats={alertStats}
                    type="rule"
                    loading={loading}
                  />
                </Grid>
              </Grid>
            </Card>
          </Grid>

          {/* 右侧：最近告警 */}
          <Grid item xs={12} md={5}>
            <Card sx={{ p: 3, height: '100%' }}>
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  mb: 2,
                }}
              >
                <Typography
                  variant="h5"
                  sx={{ fontWeight: 600, fontSize: '1.1rem' }}
                >
                  最近告警
                </Typography>
                <Typography
                  variant="body2"
                  sx={{
                    color: 'primary.main',
                    cursor: 'pointer',
                    '&:hover': { textDecoration: 'underline' },
                  }}
                  onClick={() => navigate('/quality/alerts')}
                >
                  查看全部
                </Typography>
              </Box>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
                {loading ? (
                  Array(5).fill(null).map((_, i) => (
                    <Skeleton
                      key={i}
                      variant="rectangular"
                      height={50}
                      sx={{ borderRadius: 1.5 }}
                    />
                  ))
                ) : recentAlerts.length > 0 ? (
                  recentAlerts.map((alert) => (
                    <AlertCard
                      key={alert.alertId}
                      alert={alert}
                      compact
                      onClick={() => navigate(`/quality/alerts/${alert.alertId}`)}
                    />
                  ))
                ) : (
                  <Box
                    sx={{
                      p: 4,
                      textAlign: 'center',
                      backgroundColor: 'rgba(255, 255, 255, 0.02)',
                      borderRadius: 2,
                    }}
                  >
                    <HealthyIcon sx={{ fontSize: 48, color: '#4ade80', mb: 1 }} />
                    <Typography variant="body2" color="text.secondary">
                      暂无告警，系统运行正常
                    </Typography>
                  </Box>
                )}
              </Box>
            </Card>
          </Grid>
        </Grid>

        {/* 系统状态详情 */}
        {status && (
          <Card sx={{ mt: 3, p: 3 }}>
            <Typography
              variant="h5"
              sx={{ mb: 3, fontWeight: 600, fontSize: '1.1rem' }}
            >
              系统状态详情
            </Typography>
            <Grid container spacing={3}>
              {/* 规则统计 */}
              <Grid item xs={12} sm={4}>
                <Box
                  sx={{
                    p: 2,
                    borderRadius: 2,
                    backgroundColor: 'rgba(255, 255, 255, 0.02)',
                    border: '1px solid rgba(255, 255, 255, 0.06)',
                  }}
                >
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                    规则引擎
                  </Typography>
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="caption" color="text.disabled">
                        总规则数
                      </Typography>
                      <Typography variant="caption" sx={{ fontFamily: "'JetBrains Mono', monospace" }}>
                        {status.rules?.totalRules || 0}
                      </Typography>
                    </Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="caption" color="text.disabled">
                        启用规则
                      </Typography>
                      <Typography variant="caption" sx={{ fontFamily: "'JetBrains Mono', monospace", color: '#4ade80' }}>
                        {status.rules?.enabledRules || 0}
                      </Typography>
                    </Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="caption" color="text.disabled">
                        聚合规则
                      </Typography>
                      <Typography variant="caption" sx={{ fontFamily: "'JetBrains Mono', monospace" }}>
                        {status.rules?.aggregateRules || 0}
                      </Typography>
                    </Box>
                  </Box>
                </Box>
              </Grid>

              {/* 窗口统计 */}
              <Grid item xs={12} sm={4}>
                <Box
                  sx={{
                    p: 2,
                    borderRadius: 2,
                    backgroundColor: 'rgba(255, 255, 255, 0.02)',
                    border: '1px solid rgba(255, 255, 255, 0.06)',
                  }}
                >
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                    窗口管理
                  </Typography>
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="caption" color="text.disabled">
                        活跃窗口
                      </Typography>
                      <Typography variant="caption" sx={{ fontFamily: "'JetBrains Mono', monospace" }}>
                        {status.windows?.activeWindows || 0}
                      </Typography>
                    </Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="caption" color="text.disabled">
                        已刷新窗口
                      </Typography>
                      <Typography variant="caption" sx={{ fontFamily: "'JetBrains Mono', monospace" }}>
                        {status.windows?.flushedWindows || 0}
                      </Typography>
                    </Box>
                  </Box>
                </Box>
              </Grid>

              {/* 指标统计 */}
              <Grid item xs={12} sm={4}>
                <Box
                  sx={{
                    p: 2,
                    borderRadius: 2,
                    backgroundColor: 'rgba(255, 255, 255, 0.02)',
                    border: '1px solid rgba(255, 255, 255, 0.06)',
                  }}
                >
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                    指标落库
                  </Typography>
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="caption" color="text.disabled">
                        已保存
                      </Typography>
                      <Typography variant="caption" sx={{ fontFamily: "'JetBrains Mono', monospace", color: '#4ade80' }}>
                        {status.metrics?.savedCount || 0}
                      </Typography>
                    </Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="caption" color="text.disabled">
                        错误数
                      </Typography>
                      <Typography
                        variant="caption"
                        sx={{
                          fontFamily: "'JetBrains Mono', monospace",
                          color: (status.metrics?.errorCount || 0) > 0 ? '#f87171' : 'text.secondary',
                        }}
                      >
                        {status.metrics?.errorCount || 0}
                      </Typography>
                    </Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="caption" color="text.disabled">
                        队列大小
                      </Typography>
                      <Typography variant="caption" sx={{ fontFamily: "'JetBrains Mono', monospace" }}>
                        {status.metrics?.queueSize || 0}
                      </Typography>
                    </Box>
                  </Box>
                </Box>
              </Grid>
            </Grid>
          </Card>
        )}
      </Container>
    </Box>
  );
};

export default QualityDashboardPage;

