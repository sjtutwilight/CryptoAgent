"""统一配置加载与默认值管理"""
from __future__ import annotations

import os
from typing import Any, Dict


def get_default_config() -> Dict[str, Any]:
    """获取默认配置，从环境变量读取"""
    clickhouse_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    flink_port = os.getenv("FLINK_JOBMANAGER_PORT", "8081")
    
    return {
        # ClickHouse 配置
        "clickhouse_http": f"http://localhost:{clickhouse_port}",
        "clickhouse_user": os.getenv("CLICKHOUSE_USER", ""),
        "clickhouse_password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        
        # Kafka 配置
        "kafka_broker": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        "kafka_wait_timeout": 60,
        "kafka_wait_interval": 5,
        "kafka_max_messages": 50,
        
        # Flink 配置
        "flink_rest": f"http://localhost:{flink_port}",
        "flink_wait_timeout": 60,
        
        # DataInjector 配置
        "datainjector_api": None,
        "datainjector_container": os.getenv("DATAINJECTOR_CONTAINER", "datainjector-worker"),
        "datainjector_token": None,

        # 构建配置
        "build_jar": False,
        "jar_path": "process/aggregator/target/aggregator-1.0-SNAPSHOT.jar",
        
        # 清理配置
        "clean_clickhouse": True,
        "skip_clean_clickhouse": False,
        "cancel_job": True,
        "keep_job": False,

        # 故障回归默认配置
        "role_ids": [],
        "role_config_yaml": "datainjector/worker/configs/config.yaml",
        "fault_mode": "mock",  # mock | real
        "fault_case": "disconnect_reconnect",
        "fault_action": "role_restart",  # role_restart | container_pause | noop
        "fault_pause_seconds": 8,
        "observe_seconds": 20,
        "log_since_seconds": 120,
        "fault_log_tail_lines": 5000,
        "expect_backfill": False,
        "fault_rules": {},
        "mock_provider_base_url": os.getenv("MOCK_PROVIDER_BASE_URL", "http://localhost:8090"),
        "require_mock_provider": False,
        "fault_target_container": "",
        "fault_worker_container": os.getenv("DATAINJECTOR_CONTAINER", "datainjector-worker"),
        "apply_roles_before_inject": True,
        "cleanup_stop_roles": False,
    }
