import React, { useEffect, useRef, useState } from 'react';
import { Box, Typography, ToggleButton, ToggleButtonGroup, CircularProgress } from '@mui/material';
import {
  ArrowUpward as UpIcon,
  ArrowDownward as DownIcon,
} from '@mui/icons-material';
import * as d3 from 'd3';

/**
 * 血缘图组件
 * 使用 D3.js 渲染树形血缘关系图
 */

// 节点类型颜色映射
const typeColors = {
  TABLE: '#60a5fa',
  TOPIC: '#4ade80',
  JOB: '#fbbf24',
  CONTRACT: '#f472b6',
  POOL: '#a78bfa',
  DATABASE: '#64ffda',
  DEFAULT: '#a0a0b0',
};

const LineageGraph = ({
  data,
  loading,
  error,
  direction,
  onDirectionChange,
  onNodeClick,
  width = 800,
  height = 500,
}) => {
  const svgRef = useRef(null);
  const [hoveredNode, setHoveredNode] = useState(null);

  useEffect(() => {
    if (!data || !svgRef.current) return;

    // 清空之前的内容
    d3.select(svgRef.current).selectAll('*').remove();

    const svg = d3.select(svgRef.current);
    const margin = { top: 40, right: 120, bottom: 40, left: 120 };
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;

    // 创建容器组
    const g = svg
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // 创建树形布局
    const treeLayout = d3.tree().size([innerHeight, innerWidth]);

    // 转换数据为层级结构
    const root = d3.hierarchy(data);
    treeLayout(root);

    // 绘制连接线
    const linkGenerator = d3
      .linkHorizontal()
      .x((d) => d.y)
      .y((d) => d.x);

    g.selectAll('.link')
      .data(root.links())
      .enter()
      .append('path')
      .attr('class', 'link')
      .attr('d', linkGenerator)
      .attr('fill', 'none')
      .attr('stroke', 'rgba(100, 255, 218, 0.3)')
      .attr('stroke-width', 2)
      .attr('stroke-dasharray', function () {
        const length = this.getTotalLength();
        return `${length} ${length}`;
      })
      .attr('stroke-dashoffset', function () {
        return this.getTotalLength();
      })
      .transition()
      .duration(800)
      .attr('stroke-dashoffset', 0);

    // 绘制节点组
    const nodes = g
      .selectAll('.node')
      .data(root.descendants())
      .enter()
      .append('g')
      .attr('class', 'node')
      .attr('transform', (d) => `translate(${d.y},${d.x})`)
      .style('cursor', 'pointer')
      .on('click', (event, d) => {
        if (d.data.id && onNodeClick) {
          onNodeClick(d.data);
        }
      })
      .on('mouseenter', (event, d) => {
        setHoveredNode(d.data);
      })
      .on('mouseleave', () => {
        setHoveredNode(null);
      });

    // 节点圆形背景
    nodes
      .append('circle')
      .attr('r', 0)
      .attr('fill', (d) => {
        const color = typeColors[d.data.type] || typeColors.DEFAULT;
        return `${color}20`;
      })
      .attr('stroke', (d) => typeColors[d.data.type] || typeColors.DEFAULT)
      .attr('stroke-width', 2)
      .transition()
      .duration(500)
      .delay((d, i) => i * 100)
      .attr('r', (d) => (d.depth === 0 ? 24 : 18));

    // 节点类型图标（简化为首字母）
    nodes
      .append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', '0.35em')
      .attr('fill', (d) => typeColors[d.data.type] || typeColors.DEFAULT)
      .attr('font-size', (d) => (d.depth === 0 ? '12px' : '10px'))
      .attr('font-weight', 600)
      .attr('font-family', "'JetBrains Mono', monospace")
      .attr('opacity', 0)
      .text((d) => (d.data.type || 'U')[0])
      .transition()
      .duration(500)
      .delay((d, i) => i * 100 + 200)
      .attr('opacity', 1);

    // 节点名称标签
    nodes
      .append('text')
      .attr('x', (d) => (d.depth === 0 ? 0 : d.children ? -30 : 30))
      .attr('y', (d) => (d.depth === 0 ? 40 : 0))
      .attr('text-anchor', (d) =>
        d.depth === 0 ? 'middle' : d.children ? 'end' : 'start'
      )
      .attr('dy', '0.35em')
      .attr('fill', '#e8e8f0')
      .attr('font-size', '11px')
      .attr('font-weight', 500)
      .attr('opacity', 0)
      .text((d) => {
        const name = d.data.name || 'Unknown';
        return name.length > 20 ? name.substring(0, 18) + '...' : name;
      })
      .transition()
      .duration(500)
      .delay((d, i) => i * 100 + 300)
      .attr('opacity', 1);

    // 关系类型标签（在连接线上）
    g.selectAll('.relation-label')
      .data(root.links().filter((l) => l.target.data.relationType))
      .enter()
      .append('text')
      .attr('class', 'relation-label')
      .attr('x', (d) => (d.source.y + d.target.y) / 2)
      .attr('y', (d) => (d.source.x + d.target.x) / 2 - 8)
      .attr('text-anchor', 'middle')
      .attr('fill', 'rgba(100, 255, 218, 0.6)')
      .attr('font-size', '9px')
      .attr('font-family', "'JetBrains Mono', monospace")
      .text((d) => d.target.data.relationType);

  }, [data, width, height, onNodeClick]);

  // 加载状态
  if (loading) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          height: height,
          gap: 2,
        }}
      >
        <CircularProgress size={40} sx={{ color: 'primary.main' }} />
        <Typography variant="body2" color="text.secondary">
          正在加载血缘数据...
        </Typography>
      </Box>
    );
  }

  // 错误状态
  if (error) {
    return (
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: height,
          color: 'error.main',
        }}
      >
        <Typography variant="body2">{error}</Typography>
      </Box>
    );
  }

  // 空数据状态
  if (!data) {
    return (
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: height,
          color: 'text.disabled',
        }}
      >
        <Typography variant="body2">暂无血缘数据</Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ position: 'relative' }}>
      {/* 方向切换 */}
      <Box
        sx={{
          position: 'absolute',
          top: 8,
          right: 8,
          zIndex: 10,
        }}
      >
        <ToggleButtonGroup
          value={direction}
          exclusive
          onChange={(e, val) => val && onDirectionChange?.(val)}
          size="small"
          sx={{
            backgroundColor: 'rgba(0, 0, 0, 0.3)',
            '& .MuiToggleButton-root': {
              color: 'text.secondary',
              borderColor: 'rgba(255, 255, 255, 0.1)',
              px: 1.5,
              py: 0.5,
              '&.Mui-selected': {
                color: 'primary.main',
                backgroundColor: 'rgba(100, 255, 218, 0.1)',
              },
            },
          }}
        >
          <ToggleButton value="up">
            <UpIcon sx={{ fontSize: 16, mr: 0.5 }} />
            上游
          </ToggleButton>
          <ToggleButton value="down">
            <DownIcon sx={{ fontSize: 16, mr: 0.5 }} />
            下游
          </ToggleButton>
        </ToggleButtonGroup>
      </Box>

      {/* 悬停节点信息 */}
      {hoveredNode && (
        <Box
          sx={{
            position: 'absolute',
            top: 8,
            left: 8,
            zIndex: 10,
            p: 1.5,
            backgroundColor: 'rgba(22, 22, 31, 0.95)',
            borderRadius: 1.5,
            border: '1px solid rgba(100, 255, 218, 0.2)',
            maxWidth: 250,
          }}
        >
          <Typography
            variant="body2"
            sx={{ fontWeight: 600, color: 'text.primary', mb: 0.5 }}
          >
            {hoveredNode.name}
          </Typography>
          <Typography
            variant="caption"
            sx={{
              color: typeColors[hoveredNode.type] || typeColors.DEFAULT,
              fontFamily: "'JetBrains Mono', monospace",
            }}
          >
            {hoveredNode.type}
          </Typography>
          {hoveredNode.relationType && (
            <Typography
              variant="caption"
              sx={{ display: 'block', color: 'text.secondary', mt: 0.5 }}
            >
              关系: {hoveredNode.relationType}
            </Typography>
          )}
        </Box>
      )}

      {/* SVG 画布 */}
      <svg
        ref={svgRef}
        width={width}
        height={height}
        style={{
          backgroundColor: 'rgba(255, 255, 255, 0.01)',
          borderRadius: 8,
          border: '1px solid rgba(255, 255, 255, 0.06)',
        }}
      />
    </Box>
  );
};

export default LineageGraph;

