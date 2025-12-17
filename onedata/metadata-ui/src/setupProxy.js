const { createProxyMiddleware } = require('http-proxy-middleware');

/**
 * 开发环境代理配置
 * 支持多个后端服务的API代理
 */
module.exports = function(app) {
  // 元数据服务API代理 (metadata-core: 8095)
  app.use(
    '/v1/metadata',
    createProxyMiddleware({
      target: 'http://localhost:8095',
      changeOrigin: true,
    })
  );

  // 数据质量引擎API代理 (quality-engine: 8096)
  app.use(
    '/api/quality',
    createProxyMiddleware({
      target: 'http://localhost:8096',
      changeOrigin: true,
    })
  );
};

