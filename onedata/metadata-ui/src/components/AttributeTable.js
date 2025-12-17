import React, { useState } from 'react';
import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  Collapse,
  IconButton,
  Chip,
  Tooltip,
} from '@mui/material';
import {
  KeyboardArrowDown as ExpandIcon,
  KeyboardArrowUp as CollapseIcon,
  ContentCopy as CopyIcon,
} from '@mui/icons-material';

/**
 * 属性表格组件
 * 展示元数据实体的属性列表，支持 JSON 值展开
 */

// JSON 值预览组件
const JsonPreview = ({ value, maxLength = 50 }) => {
  const [expanded, setExpanded] = useState(false);
  
  let parsed;
  let isJson = false;
  
  try {
    parsed = JSON.parse(value);
    isJson = typeof parsed === 'object' && parsed !== null;
  } catch {
    parsed = value;
  }

  // 非 JSON 或简短值直接显示
  if (!isJson || value.length <= maxLength) {
    return (
      <Typography
        variant="body2"
        sx={{
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: '0.75rem',
          color: 'text.secondary',
          wordBreak: 'break-all',
        }}
      >
        {String(value)}
      </Typography>
    );
  }

  // JSON 值支持展开/收起
  const preview = value.substring(0, maxLength) + '...';

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1 }}>
        <IconButton
          size="small"
          onClick={() => setExpanded(!expanded)}
          sx={{ p: 0.25, color: 'primary.main' }}
        >
          {expanded ? (
            <CollapseIcon sx={{ fontSize: 16 }} />
          ) : (
            <ExpandIcon sx={{ fontSize: 16 }} />
          )}
        </IconButton>
        <Typography
          variant="body2"
          sx={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: '0.75rem',
            color: 'text.secondary',
            cursor: 'pointer',
          }}
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? '' : preview}
        </Typography>
      </Box>
      <Collapse in={expanded}>
        <Box
          sx={{
            mt: 1,
            p: 1.5,
            backgroundColor: 'rgba(0, 0, 0, 0.3)',
            borderRadius: 1,
            overflow: 'auto',
            maxHeight: 300,
          }}
        >
          <pre
            style={{
              margin: 0,
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.7rem',
              color: '#a0a0b0',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-all',
            }}
          >
            {JSON.stringify(parsed, null, 2)}
          </pre>
        </Box>
      </Collapse>
    </Box>
  );
};

const AttributeTable = ({ attributes = [] }) => {
  // 复制值到剪贴板
  const handleCopy = (value) => {
    navigator.clipboard.writeText(value);
  };

  if (attributes.length === 0) {
    return (
      <Box
        sx={{
          p: 3,
          textAlign: 'center',
          backgroundColor: 'rgba(255, 255, 255, 0.02)',
          borderRadius: 1.5,
        }}
      >
        <Typography variant="body2" color="text.disabled">
          暂无属性数据
        </Typography>
      </Box>
    );
  }

  // 按 level 分组
  const groupedAttributes = attributes.reduce((acc, attr) => {
    const level = attr.level || 'default';
    if (!acc[level]) acc[level] = [];
    acc[level].push(attr);
    return acc;
  }, {});

  return (
    <Box>
      {Object.entries(groupedAttributes).map(([level, attrs]) => (
        <Box key={level} sx={{ mb: 3 }}>
          {/* 分组标题 */}
          {level !== 'default' && (
            <Box sx={{ mb: 1.5, display: 'flex', alignItems: 'center', gap: 1 }}>
              <Chip
                label={level}
                size="small"
                sx={{
                  height: 20,
                  fontSize: '0.65rem',
                  backgroundColor: 'rgba(100, 255, 218, 0.1)',
                  color: 'primary.main',
                }}
              />
              <Typography variant="caption" color="text.disabled">
                {attrs.length} 项
              </Typography>
            </Box>
          )}

          {/* 属性表格 */}
          <TableContainer
            sx={{
              backgroundColor: 'rgba(255, 255, 255, 0.02)',
              borderRadius: 1.5,
              border: '1px solid rgba(255, 255, 255, 0.06)',
            }}
          >
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell sx={{ width: '30%' }}>属性名</TableCell>
                  <TableCell>属性值</TableCell>
                  <TableCell sx={{ width: 48 }} />
                </TableRow>
              </TableHead>
              <TableBody>
                {attrs.map((attr, index) => (
                  <TableRow
                    key={`${attr.key}-${index}`}
                    sx={{
                      '&:last-child td': { borderBottom: 0 },
                      '&:hover': {
                        backgroundColor: 'rgba(100, 255, 218, 0.03)',
                      },
                    }}
                  >
                    <TableCell>
                      <Typography
                        variant="body2"
                        sx={{
                          fontFamily: "'JetBrains Mono', monospace",
                          fontSize: '0.75rem',
                          color: 'primary.main',
                          fontWeight: 500,
                        }}
                      >
                        {attr.key}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <JsonPreview value={attr.valueJson || ''} />
                    </TableCell>
                    <TableCell>
                      <Tooltip title="复制值">
                        <IconButton
                          size="small"
                          onClick={() => handleCopy(attr.valueJson || '')}
                          sx={{
                            color: 'text.disabled',
                            '&:hover': { color: 'primary.main' },
                          }}
                        >
                          <CopyIcon sx={{ fontSize: 14 }} />
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      ))}
    </Box>
  );
};

export default AttributeTable;

