import React from 'react';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  Box,
  AppBar,
  Toolbar,
  Typography,
  IconButton,
  Tooltip,
  Link,
} from '@mui/material';
import {
  Api as ApiIcon,
  DarkMode as DarkModeIcon,
} from '@mui/icons-material';

/**
 * 数据治理平台布局组件
 * 包含顶部导航栏和内容区域
 */
const Layout = () => {
  const navigate = useNavigate();
  const location = useLocation();

  // 导航项配置
  const navItems = [
    { path: '/', label: '元数据发现' },
    { path: '/quality', label: '质量监控' },
    { path: '/quality/alerts', label: '告警中心' },
    { path: '/quality/rules', label: '规则管理' },
  ];

  // 判断当前路径是否匹配导航项
  const isActive = (path) => {
    if (path === '/') return location.pathname === '/';
    return location.pathname.startsWith(path);
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      {/* 顶部导航栏 */}
      <AppBar
        position="sticky"
        elevation={0}
        sx={{
          backgroundColor: 'rgba(10, 10, 15, 0.85)',
          backdropFilter: 'blur(12px)',
          borderBottom: '1px solid rgba(255, 255, 255, 0.06)',
        }}
      >
        <Toolbar sx={{ px: { xs: 2, md: 3 } }}>
          {/* Logo */}
          <Box
            onClick={() => navigate('/')}
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1.5,
              cursor: 'pointer',
              mr: 4,
            }}
          >
            {/* Logo 图标 */}
            <Box
              sx={{
                width: 32,
                height: 32,
                borderRadius: 1.5,
                background: 'linear-gradient(135deg, #64ffda 0%, #60a5fa 100%)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontWeight: 700,
                fontSize: '1rem',
                color: '#0a0a0f',
              }}
            >
              DG
            </Box>
            <Typography
              variant="h6"
              sx={{
                fontWeight: 600,
                fontSize: '1rem',
                color: 'text.primary',
                display: { xs: 'none', sm: 'block' },
              }}
            >
              数据治理
              <Box
                component="span"
                sx={{
                  color: 'primary.main',
                  ml: 0.5,
                }}
              >
                平台
              </Box>
            </Typography>
          </Box>

          {/* 导航链接 */}
          <Box sx={{ display: 'flex', gap: 0.5 }}>
            {navItems.map((item) => (
              <Link
                key={item.path}
                component="button"
                onClick={() => navigate(item.path)}
                sx={{
                  px: 2,
                  py: 1,
                  borderRadius: 1,
                  textDecoration: 'none',
                  color: isActive(item.path) ? 'primary.main' : 'text.secondary',
                  backgroundColor: isActive(item.path)
                    ? 'rgba(100, 255, 218, 0.1)'
                    : 'transparent',
                  fontSize: '0.875rem',
                  fontWeight: 500,
                  transition: 'all 0.2s',
                  whiteSpace: 'nowrap',
                  '&:hover': {
                    backgroundColor: 'rgba(100, 255, 218, 0.08)',
                    color: 'primary.main',
                  },
                }}
              >
                {item.label}
              </Link>
            ))}
          </Box>

          {/* 右侧操作区 */}
          <Box sx={{ ml: 'auto', display: 'flex', alignItems: 'center', gap: 0.5 }}>
            {/* API 文档链接 */}
            <Tooltip title="API 文档">
              <IconButton
                component="a"
                href="/swagger-ui.html"
                target="_blank"
                sx={{ color: 'text.secondary' }}
              >
                <ApiIcon fontSize="small" />
              </IconButton>
            </Tooltip>

            {/* 主题切换（预留） */}
            <Tooltip title="主题">
              <IconButton sx={{ color: 'text.secondary' }}>
                <DarkModeIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
        </Toolbar>
      </AppBar>

      {/* 主内容区 */}
      <Box component="main" sx={{ flex: 1 }}>
        <Outlet />
      </Box>

      {/* 页脚 */}
      <Box
        component="footer"
        sx={{
          py: 2,
          px: 3,
          borderTop: '1px solid rgba(255, 255, 255, 0.06)',
          backgroundColor: 'rgba(255, 255, 255, 0.02)',
        }}
      >
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            maxWidth: 'xl',
            mx: 'auto',
          }}
        >
          <Typography variant="caption" color="text.disabled">
            © 2024 Twilight Data Platform · 数据治理平台 v0.1.0
          </Typography>
          <Typography variant="caption" color="text.disabled">
            元数据管理 · 数据质量监控
          </Typography>
        </Box>
      </Box>
    </Box>
  );
};

export default Layout;

