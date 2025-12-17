import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import DiscoveryPage from './pages/DiscoveryPage';
import EntityDetailPage from './pages/EntityDetailPage';
import QualityDashboardPage from './pages/QualityDashboardPage';
import AlertListPage from './pages/AlertListPage';
import RuleListPage from './pages/RuleListPage';

/**
 * 数据治理平台前端应用
 * 路由配置
 */
function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        {/* 元数据发现页面 */}
        <Route index element={<DiscoveryPage />} />
        
        {/* 实体详情页面 */}
        <Route path="entity/:entityId" element={<EntityDetailPage />} />
        
        {/* 质量监控模块 */}
        <Route path="quality">
          {/* 质量监控仪表盘 */}
          <Route index element={<QualityDashboardPage />} />
          {/* 告警列表 */}
          <Route path="alerts" element={<AlertListPage />} />
          {/* 告警详情（复用告警列表页面，后续可扩展） */}
          <Route path="alerts/:alertId" element={<AlertListPage />} />
          {/* 规则管理 */}
          <Route path="rules" element={<RuleListPage />} />
        </Route>
        
        {/* 404 重定向到首页 */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}

export default App;

