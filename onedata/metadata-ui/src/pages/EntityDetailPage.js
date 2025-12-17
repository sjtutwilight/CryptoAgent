import React, { useState, useEffect, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Grid,
  Tabs,
  Tab,
  Chip,
  IconButton,
  Tooltip,
  Breadcrumbs,
  Link,
  CircularProgress,
  Alert,
  Divider,
} from '@mui/material';
import {
  ArrowBack as BackIcon,
  ContentCopy as CopyIcon,
  OpenInNew as OpenIcon,
  Refresh as RefreshIcon,
  AccountTree as LineageIcon,
  Settings as AttributeIcon,
  Timeline as EventIcon,
  Assessment as QualityIcon,
} from '@mui/icons-material';
import { formatDistanceToNow } from 'date-fns';
import { zhCN } from 'date-fns/locale';
import StatusBadge from '../components/StatusBadge';
import DomainIcon, { getDomainLabel } from '../components/DomainIcon';
import TypeBadge from '../components/TypeBadge';
import QualityGauge from '../components/QualityGauge';
import LineageGraph from '../components/LineageGraph';
import AttributeTable from '../components/AttributeTable';
import EventTimeline from '../components/EventTimeline';
import { getEntityDetail, getEntityLineage } from '../services/api';

/**
 * 元数据实体详情页面
 * 展示实体的完整信息、属性、血缘、事件和质量指标
 */

// Tab 面板组件
const TabPanel = ({ children, value, index, ...props }) => (
  <Box
    role="tabpanel"
    hidden={value !== index}
    id={`tabpanel-${index}`}
    aria-labelledby={`tab-${index}`}
    {...props}
  >
    {value === index && <Box sx={{ pt: 3 }}>{children}</Box>}
  </Box>
);

const EntityDetailPage = () => {
  const { entityId } = useParams();
  const navigate = useNavigate();

  // 实体详情
  const [entity, setEntity] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // 血缘数据
  const [lineage, setLineage] = useState(null);
  const [lineageDirection, setLineageDirection] = useState('down');
  const [lineageLoading, setLineageLoading] = useState(false);
  const [lineageError, setLineageError] = useState(null);

  // 当前 Tab
  const [activeTab, setActiveTab] = useState(0);

  // 加载实体详情
  const loadEntity = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getEntityDetail(entityId);
      setEntity(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [entityId]);

  // 加载血缘数据
  const loadLineage = useCallback(async (direction) => {
    setLineageLoading(true);
    setLineageError(null);
    try {
      const data = await getEntityLineage(entityId, direction);
      setLineage(data);
    } catch (err) {
      setLineageError(err.message);
    } finally {
      setLineageLoading(false);
    }
  }, [entityId]);

  // 初始加载
  useEffect(() => {
    loadEntity();
  }, [loadEntity]);

  // 切换到血缘 Tab 时加载血缘数据
  useEffect(() => {
    if (activeTab === 1 && !lineage && !lineageLoading) {
      loadLineage(lineageDirection);
    }
  }, [activeTab, lineage, lineageLoading, loadLineage, lineageDirection]);

  // 切换血缘方向
  const handleDirectionChange = useCallback((direction) => {
    setLineageDirection(direction);
    loadLineage(direction);
  }, [loadLineage]);

  // 复制到剪贴板
  const handleCopy = (text) => {
    navigator.clipboard.writeText(text);
  };

  // 格式化时间
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

  // 加载状态
  if (loading) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          minHeight: '60vh',
          gap: 2,
        }}
      >
        <CircularProgress size={48} sx={{ color: 'primary.main' }} />
        <Typography variant="body1" color="text.secondary">
          正在加载实体详情...
        </Typography>
      </Box>
    );
  }

  // 错误状态
  if (error) {
    return (
      <Container maxWidth="lg" sx={{ py: 4 }}>
        <Alert
          severity="error"
          action={
            <IconButton size="small" color="inherit" onClick={loadEntity}>
              <RefreshIcon />
            </IconButton>
          }
          sx={{
            backgroundColor: 'rgba(248, 113, 113, 0.1)',
            border: '1px solid rgba(248, 113, 113, 0.3)',
          }}
        >
          {error}
        </Alert>
      </Container>
    );
  }

  if (!entity) return null;

  return (
    <Box sx={{ minHeight: '100vh', pb: 4 }}>
      {/* 页面头部 */}
      <Box
        sx={{
          background: 'linear-gradient(180deg, rgba(100, 255, 218, 0.05) 0%, transparent 100%)',
          borderBottom: '1px solid rgba(255, 255, 255, 0.06)',
          py: 3,
        }}
      >
        <Container maxWidth="xl">
          {/* 面包屑导航 */}
          <Breadcrumbs
            sx={{ mb: 2, '& .MuiBreadcrumbs-separator': { color: 'text.disabled' } }}
          >
            <Link
              component="button"
              variant="body2"
              onClick={() => navigate('/')}
              sx={{
                color: 'text.secondary',
                textDecoration: 'none',
                '&:hover': { color: 'primary.main' },
              }}
            >
              元数据发现
            </Link>
            <Typography variant="body2" color="text.primary">
              {entity.name}
            </Typography>
          </Breadcrumbs>

          {/* 实体标题区 */}
          <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 2 }}>
            <IconButton
              onClick={() => navigate(-1)}
              sx={{
                color: 'text.secondary',
                '&:hover': { color: 'primary.main', backgroundColor: 'rgba(100, 255, 218, 0.1)' },
              }}
            >
              <BackIcon />
            </IconButton>

            <DomainIcon domain={entity.domain} size="large" />

            <Box sx={{ flex: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 1 }}>
                <Typography
                  variant="h1"
                  sx={{
                    fontSize: { xs: '1.5rem', md: '1.75rem' },
                    fontWeight: 700,
                    color: 'text.primary',
                  }}
                >
                  {entity.name}
                </Typography>
                <StatusBadge status={entity.status} />
              </Box>

              <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
                <TypeBadge type={entity.type} />
                <Chip
                  label={getDomainLabel(entity.domain)}
                  size="small"
                  variant="outlined"
                  sx={{ borderColor: 'rgba(255, 255, 255, 0.2)' }}
                />
                {entity.platform && (
                  <Typography
                    variant="body2"
                    sx={{
                      color: 'text.secondary',
                      fontFamily: "'JetBrains Mono', monospace",
                      fontSize: '0.8rem',
                    }}
                  >
                    {entity.platform}
                  </Typography>
                )}
                <Typography variant="body2" color="text.disabled">
                  更新于 {formatTime(entity.updatedAt)}
                </Typography>
              </Box>
            </Box>

            <Box sx={{ display: 'flex', gap: 1 }}>
              <Tooltip title="刷新">
                <IconButton onClick={loadEntity} sx={{ color: 'text.secondary' }}>
                  <RefreshIcon />
                </IconButton>
              </Tooltip>
            </Box>
          </Box>

          {/* 定位符 */}
          {entity.locator && (
            <Box
              sx={{
                mt: 2,
                ml: 7,
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                p: 1.5,
                backgroundColor: 'rgba(255, 255, 255, 0.03)',
                borderRadius: 1.5,
                border: '1px solid rgba(255, 255, 255, 0.06)',
              }}
            >
              <Typography
                variant="body2"
                sx={{
                  flex: 1,
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.8rem',
                  color: 'text.secondary',
                  wordBreak: 'break-all',
                }}
              >
                {entity.locator}
              </Typography>
              <Tooltip title="复制定位符">
                <IconButton
                  size="small"
                  onClick={() => handleCopy(entity.locator)}
                  sx={{ color: 'text.secondary' }}
                >
                  <CopyIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
          )}

          {/* 标签 */}
          {entity.tags?.length > 0 && (
            <Box sx={{ mt: 2, ml: 7, display: 'flex', flexWrap: 'wrap', gap: 0.75 }}>
              {entity.tags.map((tag) => (
                <Chip
                  key={tag}
                  label={tag}
                  size="small"
                  sx={{
                    height: 22,
                    fontSize: '0.7rem',
                    backgroundColor: 'rgba(100, 255, 218, 0.1)',
                    color: 'primary.main',
                    '&:hover': { backgroundColor: 'rgba(100, 255, 218, 0.2)' },
                  }}
                />
              ))}
            </Box>
          )}
        </Container>
      </Box>

      {/* 主内容区 */}
      <Container maxWidth="xl" sx={{ mt: 3 }}>
        <Grid container spacing={3}>
          {/* 左侧：详情信息 */}
          <Grid item xs={12} lg={8}>
            {/* Tab 导航 */}
            <Tabs
              value={activeTab}
              onChange={(e, v) => setActiveTab(v)}
              sx={{
                borderBottom: '1px solid rgba(255, 255, 255, 0.06)',
                '& .MuiTab-root': {
                  minHeight: 48,
                  textTransform: 'none',
                },
              }}
            >
              <Tab
                icon={<AttributeIcon sx={{ fontSize: 18 }} />}
                iconPosition="start"
                label="属性"
              />
              <Tab
                icon={<LineageIcon sx={{ fontSize: 18 }} />}
                iconPosition="start"
                label="血缘"
              />
              <Tab
                icon={<EventIcon sx={{ fontSize: 18 }} />}
                iconPosition="start"
                label="事件"
              />
            </Tabs>

            {/* 属性 Tab */}
            <TabPanel value={activeTab} index={0}>
              <AttributeTable attributes={entity.attributes || []} />
            </TabPanel>

            {/* 血缘 Tab */}
            <TabPanel value={activeTab} index={1}>
              <LineageGraph
                data={lineage}
                loading={lineageLoading}
                error={lineageError}
                direction={lineageDirection}
                onDirectionChange={handleDirectionChange}
                onNodeClick={(node) => {
                  if (node.id && node.id !== entityId) {
                    navigate(`/entity/${node.id}`);
                  }
                }}
                width={800}
                height={450}
              />
            </TabPanel>

            {/* 事件 Tab */}
            <TabPanel value={activeTab} index={2}>
              <EventTimeline events={entity.recentEvents || []} maxItems={20} />
            </TabPanel>
          </Grid>

          {/* 右侧：质量指标和元信息 */}
          <Grid item xs={12} lg={4}>
            {/* 质量评估 */}
            <Box sx={{ mb: 3 }}>
              <Typography
                variant="h6"
                sx={{ mb: 2, fontWeight: 600, fontSize: '0.95rem' }}
              >
                <QualityIcon sx={{ fontSize: 18, mr: 1, verticalAlign: 'text-bottom' }} />
                质量评估
              </Typography>
              <QualityGauge quality={entity.quality} />
            </Box>

            <Divider sx={{ my: 3, borderColor: 'rgba(255, 255, 255, 0.06)' }} />

            {/* 元信息 */}
            <Box>
              <Typography
                variant="h6"
                sx={{ mb: 2, fontWeight: 600, fontSize: '0.95rem' }}
              >
                元信息
              </Typography>
              <Box
                sx={{
                  p: 2,
                  backgroundColor: 'rgba(255, 255, 255, 0.02)',
                  borderRadius: 2,
                  border: '1px solid rgba(255, 255, 255, 0.06)',
                }}
              >
                {[
                  { label: 'ID', value: entity.id },
                  { label: '版本', value: entity.version },
                  { label: '描述', value: entity.description },
                  { label: '协议', value: entity.protocol },
                  { label: '链 ID', value: entity.chainId },
                  { label: '合约地址', value: entity.contractAddress },
                  { label: '集群', value: entity.cluster },
                  { label: '数据库', value: entity.dbName },
                  { label: '主题', value: entity.topic },
                  { label: '作业 ID', value: entity.jobId },
                ].filter(item => item.value).map((item) => (
                  <Box
                    key={item.label}
                    sx={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'flex-start',
                      py: 1,
                      borderBottom: '1px solid rgba(255, 255, 255, 0.04)',
                      '&:last-child': { borderBottom: 0 },
                    }}
                  >
                    <Typography
                      variant="body2"
                      sx={{ color: 'text.secondary', fontSize: '0.8rem' }}
                    >
                      {item.label}
                    </Typography>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, maxWidth: '60%' }}>
                      <Typography
                        variant="body2"
                        sx={{
                          color: 'text.primary',
                          fontSize: '0.8rem',
                          fontFamily: item.label === 'ID' || item.label === '合约地址'
                            ? "'JetBrains Mono', monospace"
                            : 'inherit',
                          wordBreak: 'break-all',
                          textAlign: 'right',
                        }}
                      >
                        {item.value}
                      </Typography>
                      {(item.label === 'ID' || item.label === '合约地址') && (
                        <IconButton
                          size="small"
                          onClick={() => handleCopy(item.value)}
                          sx={{ color: 'text.disabled', p: 0.25 }}
                        >
                          <CopyIcon sx={{ fontSize: 12 }} />
                        </IconButton>
                      )}
                    </Box>
                  </Box>
                ))}
              </Box>
            </Box>
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
};

export default EntityDetailPage;

