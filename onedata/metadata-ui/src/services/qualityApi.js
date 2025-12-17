import axios from 'axios';

/**
 * 数据质量引擎 API 服务层
 * 对接后端 /api/quality/* 接口
 */

// API 基础配置（quality-engine 运行在 8096 端口）
const qualityApi = axios.create({
  baseURL: '/api/quality',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// 响应拦截器：统一错误处理
qualityApi.interceptors.response.use(
  (response) => response.data,
  (error) => {
    const message = error.response?.data?.message || error.message || '请求失败';
    console.error('[Quality API Error]', message);
    return Promise.reject(new Error(message));
  }
);

/**
 * 获取系统状态概览
 * @returns {Promise<Object>} 系统状态（规则统计、窗口统计、指标统计）
 */
export const getQualityStatus = async () => {
  return qualityApi.get('/status');
};

/**
 * 获取所有规则列表
 * @returns {Promise<Array>} 规则列表
 */
export const getQualityRules = async () => {
  return qualityApi.get('/rules');
};

/**
 * 查询告警记录
 * @param {Object} params - 查询参数
 * @param {string} params.domain - 业务域
 * @param {string} params.level - 告警级别（INFO/WARNING/CRITICAL）
 * @param {string} params.ruleName - 规则名称
 * @param {string} params.start - 开始时间（ISO格式）
 * @param {string} params.end - 结束时间（ISO格式）
 * @param {number} params.page - 页码（从0开始）
 * @param {number} params.size - 每页数量
 * @returns {Promise<Object>} 分页告警记录
 */
export const getAlerts = async (params = {}) => {
  const queryParams = new URLSearchParams();
  
  if (params.domain) queryParams.append('domain', params.domain);
  if (params.level) queryParams.append('level', params.level);
  if (params.ruleName) queryParams.append('ruleName', params.ruleName);
  if (params.start) queryParams.append('start', params.start);
  if (params.end) queryParams.append('end', params.end);
  queryParams.append('page', params.page ?? 0);
  queryParams.append('size', params.size ?? 20);
  
  return qualityApi.get(`/alerts?${queryParams.toString()}`);
};

/**
 * 获取告警统计
 * @param {number} hours - 统计时间范围（小时）
 * @returns {Promise<Object>} 告警统计（按级别、域、规则）
 */
export const getAlertStats = async (hours = 24) => {
  return qualityApi.get(`/alerts/stats?hours=${hours}`);
};

/**
 * 获取单个告警详情
 * @param {string} alertId - 告警ID
 * @returns {Promise<Object>} 告警详情
 */
export const getAlertDetail = async (alertId) => {
  return qualityApi.get(`/alerts/${alertId}`);
};

/**
 * 健康检查
 * @returns {Promise<Object>} 健康状态
 */
export const getQualityHealth = async () => {
  return qualityApi.get('/health');
};

// 导出告警级别枚举
export const AlertLevel = {
  INFO: 'INFO',
  WARNING: 'WARNING',
  CRITICAL: 'CRITICAL',
};

// 导出质量维度枚举
export const QualityDimension = {
  COMPLETENESS: 'COMPLETENESS',
  TIMELINESS: 'TIMELINESS',
  ACCURACY: 'ACCURACY',
  CONSISTENCY: 'CONSISTENCY',
  SCHEMA: 'SCHEMA',
};

// 导出业务域枚举
export const DataDomain = {
  DEX_UNISWAP: 'DEX_UNISWAP',
  DEX_HYPERLIQUID: 'DEX_HYPERLIQUID',
  CEX_KLINE: 'CEX_KLINE',
  CEX_PERP_ORDERBOOK: 'CEX_PERP_ORDERBOOK',
  CEX_PERP_TRADES: 'CEX_PERP_TRADES',
  CEX_PERP_FUNDING: 'CEX_PERP_FUNDING',
};

// 域名称映射（用于显示）
export const DomainLabels = {
  DEX_UNISWAP: 'DEX Uniswap',
  DEX_HYPERLIQUID: 'DEX Hyperliquid',
  CEX_KLINE: 'CEX K线',
  CEX_PERP_ORDERBOOK: 'CEX 永续订单簿',
  CEX_PERP_TRADES: 'CEX 永续成交',
  CEX_PERP_FUNDING: 'CEX 永续资金费率',
};

// 维度名称映射（用于显示）
export const DimensionLabels = {
  COMPLETENESS: '完整性',
  TIMELINESS: '时效性',
  ACCURACY: '准确性',
  CONSISTENCY: '一致性',
  SCHEMA: '模式合规',
};

// 告警级别配置（用于UI显示）
export const AlertLevelConfig = {
  INFO: {
    label: '信息',
    color: '#60a5fa',
    bgColor: 'rgba(96, 165, 250, 0.12)',
  },
  WARNING: {
    label: '警告',
    color: '#fbbf24',
    bgColor: 'rgba(251, 191, 36, 0.12)',
  },
  CRITICAL: {
    label: '严重',
    color: '#f87171',
    bgColor: 'rgba(248, 113, 113, 0.12)',
  },
};

export default qualityApi;

