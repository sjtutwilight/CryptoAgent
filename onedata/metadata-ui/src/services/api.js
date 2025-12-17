import axios from 'axios';

/**
 * Metadata Core API 服务层
 * 对接后端 /v1/metadata/* 接口
 */

// API 基础配置
const api = axios.create({
  baseURL: '/v1/metadata',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// 响应拦截器：统一错误处理
api.interceptors.response.use(
  (response) => response.data,
  (error) => {
    const message = error.response?.data?.message || error.message || '请求失败';
    console.error('[API Error]', message);
    return Promise.reject(new Error(message));
  }
);

/**
 * 元数据实体搜索
 * @param {Object} params - 搜索参数
 * @param {string} params.keyword - 关键字
 * @param {string} params.domain - 域（如 defi, ch, flink, kafka, paimon）
 * @param {string} params.type - 类型（如 TABLE, TOPIC, JOB, CONTRACT）
 * @param {string} params.platform - 平台（如 clickhouse, kafka, ethereum）
 * @param {string} params.status - 状态（ACTIVE, INACTIVE, DEPRECATED, FAILED, UNKNOWN）
 * @param {string[]} params.tags - 标签列表
 * @param {number} params.page - 页码（从0开始）
 * @param {number} params.size - 每页数量
 * @param {string} params.sortBy - 排序字段
 * @param {string} params.sortDirection - 排序方向（ASC, DESC）
 * @returns {Promise<{content: Array, totalElements: number, totalPages: number}>}
 */
export const searchEntities = async (params = {}) => {
  const queryParams = new URLSearchParams();
  
  if (params.keyword) queryParams.append('keyword', params.keyword);
  if (params.domain) queryParams.append('domain', params.domain);
  if (params.type) queryParams.append('type', params.type);
  if (params.platform) queryParams.append('platform', params.platform);
  if (params.status) queryParams.append('status', params.status);
  if (params.tags?.length) {
    params.tags.forEach(tag => queryParams.append('tags', tag));
  }
  queryParams.append('page', params.page ?? 0);
  queryParams.append('size', params.size ?? 20);
  queryParams.append('sortBy', params.sortBy ?? 'updatedAt');
  queryParams.append('sortDirection', params.sortDirection ?? 'DESC');
  
  return api.get(`/entities?${queryParams.toString()}`);
};

/**
 * 获取实体详情
 * @param {string} entityId - 实体UUID
 * @returns {Promise<Object>} 实体详情（含属性、标签、最近事件、质量指标）
 */
export const getEntityDetail = async (entityId) => {
  return api.get(`/entities/${entityId}`);
};

/**
 * 获取实体血缘
 * @param {string} entityId - 实体UUID
 * @param {string} direction - 方向（up: 上游, down: 下游）
 * @returns {Promise<Object>} 血缘树结构
 */
export const getEntityLineage = async (entityId, direction = 'down') => {
  return api.get(`/entities/${entityId}/lineage?direction=${direction}`);
};

/**
 * 获取实体质量指标
 * @param {string} entityId - 实体UUID
 * @returns {Promise<Object>} 质量指标
 */
export const getEntityQuality = async (entityId) => {
  return api.get(`/entities/${entityId}/quality`);
};

/**
 * 获取域统计
 * @param {string} domain - 域名称
 * @returns {Promise<Object>} 域统计数据
 */
export const getDomainStats = async (domain) => {
  return api.get(`/domains/${domain}/stats`);
};

/**
 * 获取所有域的统计数据
 * @returns {Promise<Object[]>} 所有域的统计数据列表
 */
export const getAllDomainStats = async () => {
  const domains = ['defi', 'clickhouse', 'flink', 'kafka', 'paimon', 'postgres'];
  const results = await Promise.allSettled(
    domains.map(domain => getDomainStats(domain))
  );
  return results
    .filter(r => r.status === 'fulfilled')
    .map(r => r.value);
};

/**
 * 创建 SSE 实时更新订阅
 * @param {Function} onMessage - 消息回调
 * @param {Function} onError - 错误回调
 * @returns {EventSource} SSE 连接实例
 */
export const subscribeUpdates = (onMessage, onError) => {
  const eventSource = new EventSource('/v1/metadata/updates/stream');
  
  eventSource.addEventListener('metadata-update', (event) => {
    try {
      const data = JSON.parse(event.data);
      onMessage?.(data);
    } catch (e) {
      console.error('[SSE Parse Error]', e);
    }
  });
  
  eventSource.onerror = (error) => {
    console.error('[SSE Error]', error);
    onError?.(error);
  };
  
  return eventSource;
};

// 导出状态枚举
export const MetadataStatus = {
  ACTIVE: 'ACTIVE',
  INACTIVE: 'INACTIVE',
  DEPRECATED: 'DEPRECATED',
  FAILED: 'FAILED',
  UNKNOWN: 'UNKNOWN',
};

// 导出域枚举
export const MetadataDomain = {
  DEFI: 'defi',
  CLICKHOUSE: 'clickhouse',
  FLINK: 'flink',
  KAFKA: 'kafka',
  PAIMON: 'paimon',
  POSTGRES: 'postgres',
};

// 导出类型枚举
export const MetadataType = {
  TABLE: 'TABLE',
  COLUMN: 'COLUMN',
  TOPIC: 'TOPIC',
  JOB: 'JOB',
  CONTRACT: 'CONTRACT',
  POOL: 'POOL',
  DATABASE: 'DATABASE',
};

export default api;

