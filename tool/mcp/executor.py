#!/usr/bin/env python3
"""
通用命令执行器

构造并执行 shell 命令，统一处理 JSON 输出
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class ExecutionResult:
    """命令执行结果"""
    
    def __init__(self, success: bool, output: str, error: str, exit_code: int):
        self.success = success
        self.output = output
        self.error = error
        self.exit_code = exit_code
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'success': self.success,
            'output': self.output,
            'error': self.error,
            'exit_code': self.exit_code
        }
    
    def to_json(self) -> str:
        """转换为 JSON 字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


def build_command_args(command: str, args: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    构造命令参数列表
    
    Args:
        command: 命令名（如 "flink:run"）
        args: 参数字典
    
    Returns:
        完整的命令参数列表
    """
    cmd_args = [command]
    
    if not args:
        return cmd_args
    
    for key, value in args.items():
        # 处理布尔标志
        if isinstance(value, bool):
            if value:
                # 将 snake_case 转换为 kebab-case
                flag_name = key.replace('_', '-')
                cmd_args.append(f'--{flag_name}')
        # 处理列表参数
        elif isinstance(value, list):
            flag_name = key.replace('_', '-')
            for item in value:
                cmd_args.append(f'--{flag_name}')
                cmd_args.append(str(item))
        # 处理普通参数
        else:
            flag_name = key.replace('_', '-')
            cmd_args.append(f'--{flag_name}')
            cmd_args.append(str(value))
    
    return cmd_args


def execute_script(
    script_path: Path,
    command: str,
    args: Optional[Dict[str, Any]] = None,
    timeout: int = 300,
    force_json: bool = True,
    supports_output_json: bool = False
) -> ExecutionResult:
    """
    执行脚本命令
    
    Args:
        script_path: 脚本路径
        command: 命令名
        args: 命令参数
        timeout: 超时时间（秒）
        force_json: 是否强制添加 --output-json 参数
    
    Returns:
        执行结果
    """
    if not script_path.exists():
        return ExecutionResult(
            success=False,
            output='',
            error=f'Script not found: {script_path}',
            exit_code=127
        )
    
    # 构造完整命令
    cmd_args = build_command_args(command, args)
    
    # 如果脚本支持 JSON 输出且未指定，自动添加
    if force_json and supports_output_json:
        if not args or 'output_json' not in args:
            cmd_args.append('--output-json')
    
    full_cmd = [str(script_path)] + cmd_args
    
    try:
        result = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=script_path.parent.parent  # 在仓库根目录执行
        )
        
        # 尝试解析输出为 JSON
        output = result.stdout.strip()
        error = result.stderr.strip()
        
        # 如果输出是 JSON，尝试格式化
        try:
            if output:
                json_data = json.loads(output)
                output = json.dumps(json_data, ensure_ascii=False, indent=2)
        except json.JSONDecodeError:
            # 如果不是 JSON，保持原样
            pass
        
        return ExecutionResult(
            success=result.returncode == 0,
            output=output,
            error=error,
            exit_code=result.returncode
        )
    
    except subprocess.TimeoutExpired:
        return ExecutionResult(
            success=False,
            output='',
            error=f'Command timeout after {timeout} seconds',
            exit_code=124
        )
    except Exception as e:
        return ExecutionResult(
            success=False,
            output='',
            error=f'Execution error: {str(e)}',
            exit_code=1
        )


def execute_ops_command(
    repo_root: Path,
    command: str,
    args: Optional[Dict[str, Any]] = None,
    supports_output_json: bool = False
) -> ExecutionResult:
    """执行 ops.sh 命令"""
    script_path = repo_root / 'tool' / 'ops.sh'
    return execute_script(script_path, command, args, supports_output_json=supports_output_json)


def execute_test_command(
    repo_root: Path,
    command: str,
    args: Optional[Dict[str, Any]] = None,
    supports_output_json: bool = False
) -> ExecutionResult:
    """执行 test.sh 命令"""
    script_path = repo_root / 'tool' / 'test.sh'
    return execute_script(script_path, command, args, supports_output_json=supports_output_json)


def execute_orchestration_command(
    repo_root: Path,
    keywords: List[str]
) -> ExecutionResult:
    """
    执行 orchestration.sh 命令
    
    Args:
        repo_root: 仓库根目录
        keywords: 关键词列表（如 ['ingest', 'bd']）
    """
    script_path = repo_root / 'tool' / 'orchestration.sh'
    
    if not script_path.exists():
        return ExecutionResult(
            success=False,
            output='',
            error=f'Script not found: {script_path}',
            exit_code=127
        )
    
    # orchestration.sh 直接接受关键词参数
    full_cmd = [str(script_path)] + keywords
    
    try:
        result = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=repo_root
        )
        
        return ExecutionResult(
            success=result.returncode == 0,
            output=result.stdout.strip(),
            error=result.stderr.strip(),
            exit_code=result.returncode
        )
    
    except subprocess.TimeoutExpired:
        return ExecutionResult(
            success=False,
            output='',
            error='Command timeout after 300 seconds',
            exit_code=124
        )
    except Exception as e:
        return ExecutionResult(
            success=False,
            output='',
            error=f'Execution error: {str(e)}',
            exit_code=1
        )


def main():
    """命令行入口，用于测试"""
    import sys
    
    # 测试用例
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    
    # 测试 ops 命令
    print("Testing ops command: role:alive_list")
    result = execute_ops_command(
        repo_root,
        'role:alive_list',
        {'output_json': True},
        supports_output_json=True
    )
    print(result.to_json())
    print()
    
    # 测试 test 命令
    print("Testing test command: list")
    result = execute_test_command(repo_root, 'list', supports_output_json=True)
    print(result.to_json())


if __name__ == '__main__':
    main()


