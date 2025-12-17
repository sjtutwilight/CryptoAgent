import { createTheme, alpha } from '@mui/material/styles';

/**
 * 元数据管理平台主题配置
 * 设计风格：深色科技风，强调数据可视化与信息层次
 */
const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#64ffda',      // 青绿色主色调
      light: '#9effeb',
      dark: '#14b8a6',
      contrastText: '#0a0a0f',
    },
    secondary: {
      main: '#f472b6',      // 粉色辅助色
      light: '#f9a8d4',
      dark: '#db2777',
      contrastText: '#0a0a0f',
    },
    error: {
      main: '#f87171',
      light: '#fca5a5',
      dark: '#dc2626',
    },
    warning: {
      main: '#fbbf24',
      light: '#fcd34d',
      dark: '#d97706',
    },
    success: {
      main: '#4ade80',
      light: '#86efac',
      dark: '#16a34a',
    },
    info: {
      main: '#60a5fa',
      light: '#93c5fd',
      dark: '#2563eb',
    },
    background: {
      default: '#0a0a0f',
      paper: '#12121a',
    },
    text: {
      primary: '#e8e8f0',
      secondary: '#a0a0b0',
      disabled: '#606070',
    },
    divider: 'rgba(255, 255, 255, 0.08)',
  },
  typography: {
    fontFamily: "'Space Grotesk', -apple-system, BlinkMacSystemFont, sans-serif",
    h1: {
      fontSize: '2.5rem',
      fontWeight: 700,
      letterSpacing: '-0.02em',
      lineHeight: 1.2,
    },
    h2: {
      fontSize: '2rem',
      fontWeight: 600,
      letterSpacing: '-0.01em',
      lineHeight: 1.3,
    },
    h3: {
      fontSize: '1.5rem',
      fontWeight: 600,
      letterSpacing: '-0.01em',
      lineHeight: 1.4,
    },
    h4: {
      fontSize: '1.25rem',
      fontWeight: 600,
      lineHeight: 1.4,
    },
    h5: {
      fontSize: '1rem',
      fontWeight: 600,
      lineHeight: 1.5,
    },
    h6: {
      fontSize: '0.875rem',
      fontWeight: 600,
      lineHeight: 1.5,
    },
    body1: {
      fontSize: '0.9375rem',
      lineHeight: 1.6,
    },
    body2: {
      fontSize: '0.8125rem',
      lineHeight: 1.5,
    },
    caption: {
      fontSize: '0.75rem',
      lineHeight: 1.5,
      color: '#a0a0b0',
    },
    button: {
      textTransform: 'none',
      fontWeight: 500,
    },
    // 代码字体
    mono: {
      fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
      fontSize: '0.8125rem',
    },
  },
  shape: {
    borderRadius: 8,
  },
  shadows: [
    'none',
    '0 1px 2px rgba(0, 0, 0, 0.3)',
    '0 2px 4px rgba(0, 0, 0, 0.35)',
    '0 4px 8px rgba(0, 0, 0, 0.4)',
    '0 6px 12px rgba(0, 0, 0, 0.45)',
    '0 8px 16px rgba(0, 0, 0, 0.5)',
    '0 12px 24px rgba(0, 0, 0, 0.55)',
    '0 16px 32px rgba(0, 0, 0, 0.6)',
    '0 20px 40px rgba(0, 0, 0, 0.65)',
    '0 24px 48px rgba(0, 0, 0, 0.7)',
    ...Array(15).fill('0 24px 48px rgba(0, 0, 0, 0.7)'),
  ],
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          scrollbarColor: 'rgba(100, 255, 218, 0.2) rgba(255, 255, 255, 0.03)',
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 6,
          padding: '8px 20px',
          fontSize: '0.875rem',
          fontWeight: 500,
        },
        contained: {
          boxShadow: 'none',
          '&:hover': {
            boxShadow: '0 4px 12px rgba(100, 255, 218, 0.25)',
          },
        },
        outlined: {
          borderWidth: '1.5px',
          '&:hover': {
            borderWidth: '1.5px',
            backgroundColor: 'rgba(100, 255, 218, 0.08)',
          },
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundColor: '#16161f',
          backgroundImage: 'none',
          border: '1px solid rgba(255, 255, 255, 0.06)',
          borderRadius: 12,
          transition: 'all 0.2s ease-in-out',
          '&:hover': {
            borderColor: 'rgba(100, 255, 218, 0.2)',
            boxShadow: '0 8px 24px rgba(0, 0, 0, 0.4)',
          },
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          borderRadius: 6,
          fontWeight: 500,
          fontSize: '0.75rem',
        },
        filled: {
          backgroundColor: 'rgba(100, 255, 218, 0.12)',
          color: '#64ffda',
          '&:hover': {
            backgroundColor: 'rgba(100, 255, 218, 0.2)',
          },
        },
        outlined: {
          borderColor: 'rgba(100, 255, 218, 0.3)',
          color: '#64ffda',
        },
      },
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          '& .MuiOutlinedInput-root': {
            backgroundColor: 'rgba(255, 255, 255, 0.02)',
            '& fieldset': {
              borderColor: 'rgba(255, 255, 255, 0.1)',
            },
            '&:hover fieldset': {
              borderColor: 'rgba(100, 255, 218, 0.3)',
            },
            '&.Mui-focused fieldset': {
              borderColor: '#64ffda',
              borderWidth: '1.5px',
            },
          },
        },
      },
    },
    MuiSelect: {
      styleOverrides: {
        root: {
          backgroundColor: 'rgba(255, 255, 255, 0.02)',
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          borderColor: 'rgba(255, 255, 255, 0.06)',
          padding: '12px 16px',
        },
        head: {
          backgroundColor: 'rgba(255, 255, 255, 0.03)',
          fontWeight: 600,
          color: '#a0a0b0',
          fontSize: '0.75rem',
          textTransform: 'uppercase',
          letterSpacing: '0.05em',
        },
      },
    },
    MuiTableRow: {
      styleOverrides: {
        root: {
          '&:hover': {
            backgroundColor: 'rgba(100, 255, 218, 0.04)',
          },
        },
      },
    },
    MuiTooltip: {
      styleOverrides: {
        tooltip: {
          backgroundColor: '#1e1e2a',
          border: '1px solid rgba(255, 255, 255, 0.1)',
          borderRadius: 6,
          fontSize: '0.75rem',
          padding: '8px 12px',
        },
        arrow: {
          color: '#1e1e2a',
        },
      },
    },
    MuiDialog: {
      styleOverrides: {
        paper: {
          backgroundColor: '#16161f',
          border: '1px solid rgba(255, 255, 255, 0.08)',
          borderRadius: 12,
        },
      },
    },
    MuiDrawer: {
      styleOverrides: {
        paper: {
          backgroundColor: '#12121a',
          borderRight: '1px solid rgba(255, 255, 255, 0.06)',
        },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          fontWeight: 500,
          fontSize: '0.875rem',
          minHeight: 48,
          '&.Mui-selected': {
            color: '#64ffda',
          },
        },
      },
    },
    MuiTabs: {
      styleOverrides: {
        indicator: {
          backgroundColor: '#64ffda',
          height: 2,
        },
      },
    },
    MuiLinearProgress: {
      styleOverrides: {
        root: {
          backgroundColor: 'rgba(100, 255, 218, 0.1)',
          borderRadius: 4,
        },
        bar: {
          borderRadius: 4,
        },
      },
    },
    MuiBadge: {
      styleOverrides: {
        badge: {
          fontWeight: 600,
          fontSize: '0.6875rem',
        },
      },
    },
  },
});

export default theme;

