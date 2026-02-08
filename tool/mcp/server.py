#!/usr/bin/env python3
"""
DataPlatform MCP Server

为 tool/ 入口脚本提供 MCP 工具接口
使用通用包装器 + 自动命令发现机制，实现零维护成本扩展
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# 添加项目根目录到 Python 路径
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from discovery import discover_all_commands
from executor import (
    execute_ops_command,
    execute_test_command,
    execute_orchestration_command,
)


# 全局变量：缓存发现的命令
DISCOVERED_COMMANDS: Optional[Dict[str, Any]] = None


def get_discovered_commands() -> Dict[str, Any]:
    """获取已发现的命令（带缓存）"""
    global DISCOVERED_COMMANDS
    if DISCOVERED_COMMANDS is None:
        DISCOVERED_COMMANDS = discover_all_commands(REPO_ROOT)
    return DISCOVERED_COMMANDS


def get_tool_info_by_name(tool_name: str) -> Dict[str, Any]:
    """根据工具名查找脚本信息"""
    discovered = get_discovered_commands()
    for info in discovered.values():
        if info["tool_name"] == tool_name:
            return info
    raise KeyError(f"Tool not found: {tool_name}")


# 创建 MCP Server 实例
app = Server("dataplatform-tools")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """列出所有可用的 MCP tools"""
    
    # 动态发现命令
    discovered = get_discovered_commands()
    
    ops_info = discovered['ops']
    test_info = discovered['test']
    orch_info = discovered['orchestration']
    ops_commands = ops_info['commands']
    test_commands = test_info['commands']
    orch_commands = orch_info['commands']
    
    return [
        Tool(
            name=ops_info["tool_name"],
            description=ops_info["description"],
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "命令名（domain:action 格式）",
                        "enum": [cmd['name'] for cmd in ops_commands]
                    },
                    "args": {
                        "type": "object",
                        "description": "命令参数（可选）。布尔标志 true -> --flag；列表参数 -> --flag value 重复。",
                        "additionalProperties": True
                    }
                },
                "required": ["command"]
            }
        ),
        Tool(
            name=test_info["tool_name"],
            description=test_info["description"],
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "命令名",
                        "enum": [cmd['name'] for cmd in test_commands]
                    },
                    "args": {
                        "type": "object",
                        "description": "命令参数（可选）。布尔标志 true -> --flag；列表参数 -> --flag value 重复。",
                        "additionalProperties": True
                    }
                },
                "required": ["command"]
            }
        ),
        Tool(
            name=orch_info["tool_name"],
            description=orch_info["description"],
            inputSchema={
                "type": "object",
                "properties": {
                    "keywords": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "关键词列表"
                    }
                },
                "required": ["keywords"]
            }
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """处理工具调用"""
    
    try:
        if name == "ops_execute":
            command = arguments.get("command")
            args = arguments.get("args", {})
            
            if not command:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": False,
                        "error": "Missing required parameter: command"
                    }, ensure_ascii=False, indent=2)
                )]
            
            ops_info = get_tool_info_by_name("ops_execute")
            result = execute_ops_command(
                REPO_ROOT,
                command,
                args,
                supports_output_json=ops_info["supports_output_json"]
            )
            return [TextContent(type="text", text=result.to_json())]
        
        elif name == "test_execute":
            command = arguments.get("command")
            args = arguments.get("args", {})
            
            if not command:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": False,
                        "error": "Missing required parameter: command"
                    }, ensure_ascii=False, indent=2)
                )]
            
            test_info = get_tool_info_by_name("test_execute")
            result = execute_test_command(
                REPO_ROOT,
                command,
                args,
                supports_output_json=test_info["supports_output_json"]
            )
            return [TextContent(type="text", text=result.to_json())]
        
        elif name == "orchestration_execute":
            keywords = arguments.get("keywords", [])
            
            if not keywords:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": False,
                        "error": "Missing required parameter: keywords"
                    }, ensure_ascii=False, indent=2)
                )]
            
            result = execute_orchestration_command(REPO_ROOT, keywords)
            return [TextContent(type="text", text=result.to_json())]
        
        else:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "success": False,
                    "error": f"Unknown tool: {name}"
                }, ensure_ascii=False, indent=2)
            )]
    
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": False,
                "error": f"Tool execution error: {str(e)}"
            }, ensure_ascii=False, indent=2)
        )]


async def main():
    """MCP Server 主入口"""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    import anyio
    anyio.run(main)
