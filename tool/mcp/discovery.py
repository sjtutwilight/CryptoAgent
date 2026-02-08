#!/usr/bin/env python3
"""
命令自动发现模块

从入口脚本的 usage() 函数中提取可用命令列表
"""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List


SCRIPT_TOOL_NAMES = {
    "ops.sh": "ops_execute",
    "test.sh": "test_execute",
    "orchestration.sh": "orchestration_execute",
}


def extract_commands_from_usage(usage_text: str) -> List[Dict[str, str]]:
    """
    从 usage 文本中提取命令列表（容错解析）
    
    格式规范（推荐）：
      command:name      简短描述
    
    容错规则：
    - 空格数量灵活（1+ 个空格开头，2+ 个空格分隔）
    - 自动过滤空行和非命令行
    - 支持命令名包含字母、数字、冒号、下划线、连字符
    
    Args:
        usage_text: 脚本 --help 输出的文本
    
    Returns:
        命令列表，每个命令包含 name 和 description
    """
    commands = []
    lines = usage_text.split('\n')
    
    for line in lines:
        # 容错匹配：至少 1 个空格开头，命令名，至少 2 个空格，描述
        # 命令名支持：字母数字冒号下划线连字符
        match = re.match(r'^\s+([\w:_-]+)\s{2,}(.+)$', line)
        if match:
            command_name = match.group(1).strip()
            description = match.group(2).strip()
            
            commands.append({
                'name': command_name,
                'description': description
            })
    
    return commands


def load_script_metadata(script_path: Path) -> Dict[str, Any]:
    """
    读取脚本 MCP 元数据
    """
    try:
        result = subprocess.run(
            [str(script_path), '--mcp'],
            capture_output=True,
            text=True,
            timeout=5
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Timeout while getting MCP metadata from {script_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to run {script_path} --mcp: {e}")

    output = (result.stdout or result.stderr).strip()
    if not output:
        raise RuntimeError(f"Empty MCP metadata from {script_path}")

    try:
        metadata = json.loads(output)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid MCP metadata JSON from {script_path}: {e}")

    return metadata


def discover_script_commands(script_path: Path) -> Dict[str, Any]:
    """
    发现脚本的所有可用命令
    
    Args:
        script_path: 脚本文件路径
    
    Returns:
        包含脚本信息和命令列表的字典
    """
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    metadata = load_script_metadata(script_path)

    expected_tool_name = SCRIPT_TOOL_NAMES.get(script_path.name)
    if expected_tool_name and metadata.get("tool_name") != expected_tool_name:
        raise ValueError(
            f"Tool name mismatch for {script_path.name}: "
            f"expected {expected_tool_name}, got {metadata.get('tool_name')}"
        )

    if not metadata.get("description"):
        raise ValueError(f"Missing MCP description for {script_path}")

    if "supports_output_json" not in metadata:
        raise ValueError(f"Missing supports_output_json in MCP metadata for {script_path}")
    if not isinstance(metadata.get("supports_output_json"), bool):
        raise ValueError(f"supports_output_json must be boolean in MCP metadata for {script_path}")

    # 执行脚本的 --help 获取 usage
    try:
        result = subprocess.run(
            [str(script_path), '--help'],
            capture_output=True,
            text=True,
            timeout=5
        )
        usage_text = result.stdout or result.stderr
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Timeout while getting help from {script_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to run {script_path}: {e}")
    
    commands = extract_commands_from_usage(usage_text)
    
    return {
        'script': str(script_path),
        'script_name': script_path.name,
        'tool_name': metadata['tool_name'],
        'description': metadata['description'],
        'supports_output_json': metadata['supports_output_json'],
        'commands': commands,
        'command_count': len(commands)
    }


def discover_all_commands(repo_root: Path) -> Dict[str, Any]:
    """
    发现所有入口脚本的命令
    
    Args:
        repo_root: 仓库根目录
    
    Returns:
        所有脚本的命令信息
    """
    tool_dir = repo_root / 'tool'
    
    ops_info = discover_script_commands(tool_dir / 'ops.sh')
    test_info = discover_script_commands(tool_dir / 'test.sh')
    orch_info = discover_script_commands(tool_dir / 'orchestration.sh')

    script_descriptions = [ops_info['description'], test_info['description'], orch_info['description']]
    if len(set(script_descriptions)) != len(script_descriptions):
        raise ValueError("Duplicate MCP tool descriptions detected in scripts")

    description_index: Dict[str, List[str]] = {}
    for script_key, info in {
        'ops': ops_info,
        'test': test_info,
        'orchestration': orch_info,
    }.items():
        for command in info['commands']:
            description_index.setdefault(command['description'], []).append(
                f"{script_key}:{command['name']}"
            )

    duplicate_descriptions = {
        desc: refs for desc, refs in description_index.items() if len(refs) > 1
    }
    if duplicate_descriptions:
        details = "; ".join(
            f"{desc} -> {', '.join(refs)}"
            for desc, refs in duplicate_descriptions.items()
        )
        raise ValueError(f"Duplicate command descriptions detected: {details}")

    return {
        'ops': ops_info,
        'test': test_info,
        'orchestration': orch_info
    }


def main():
    """命令行入口，用于测试"""
    import json
    import sys
    
    # 获取仓库根目录
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    
    try:
        all_commands = discover_all_commands(repo_root)
        print(json.dumps(all_commands, indent=2, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({'error': str(e)}, indent=2), file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
