from __future__ import annotations

import os
import time
from typing import Dict

from automation.test.probes import file_probe, worker_probe
from automation.test.shared.core.context import RunContext
from automation.test.shared.core.result import ProbeResult, ProbeStatus
from automation.test.shared.core.scenario import ProbeCall, Scenario, Stage
from automation.test.shared.ingress import datainjector
from automation.test.shared.paths import resolve_container_path

# 使用 config.yaml 中定义的 role_id
DEFAULT_ROLE_ID = "binance-spot-link-kline-batch"
DEFAULT_TOPIC = "batch.tasks"
DEFAULT_GROUP_ID = "worker.binance.kline.batch"
DEFAULT_OUTPUT_DIR = "runtime/data/binance/spot/kline/LINKUSDT"
DEFAULT_FILENAME_PREFIX = "kline_1m"
DEFAULT_API_ENDPOINT = "https://api.binance.com/api/v3/ping"


def _default_config() -> Dict:
    return {
        "role_id": DEFAULT_ROLE_ID,
        "topic": DEFAULT_TOPIC,
        "group_id": DEFAULT_GROUP_ID,
        "output_dir": DEFAULT_OUTPUT_DIR,
        "filename_prefix": DEFAULT_FILENAME_PREFIX,
        "api_endpoint": DEFAULT_API_ENDPOINT,
        "wait_timeout": 120,
        "wait_interval": 5,
        "datainjector_container": os.getenv("DATAINJECTOR_CONTAINER", "datainjector-worker"),
        "datainjector_api": None,
        "kafka_broker": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    }


def _get_config(ctx: RunContext) -> Dict:
    cfg = _default_config()
    cfg.update(ctx.metadata or {})
    return cfg


# ==================== Probe 函数 ====================

def _probe_worker_health(ctx: RunContext) -> ProbeResult:
    """检查 Worker 容器健康状态"""
    cfg = _get_config(ctx)
    return worker_probe.check_worker_health(ctx, cfg["datainjector_container"])


def _probe_api_connectivity(ctx: RunContext) -> ProbeResult:
    """检查 Binance API 连通性"""
    cfg = _get_config(ctx)
    return worker_probe.check_api_connectivity(ctx, cfg["api_endpoint"], cfg["datainjector_container"])


def _probe_file_permissions(ctx: RunContext) -> ProbeResult:
    """检查输出目录权限"""
    cfg = _get_config(ctx)
    container_dir = resolve_container_path(cfg["output_dir"])
    return worker_probe.check_file_permissions(ctx, container_dir, cfg["datainjector_container"])


def _probe_apply_role(ctx: RunContext, state: Dict) -> ProbeResult:
    """应用 role 配置"""
    cfg = _get_config(ctx)
    role_ids = [cfg["role_id"]]
    
    result = datainjector.apply_roles_by_ids(
        ctx=ctx,
        role_ids=role_ids,
        api=cfg.get("datainjector_api"),
        container=cfg["datainjector_container"],
        token=cfg.get("datainjector_token"),
    )
    
    if result.status == ProbeStatus.SUCCESS:
        state["role_ids"] = role_ids
        state["role_id"] = role_ids[0]
    
    return result


def _probe_role_status(ctx: RunContext, state: Dict) -> ProbeResult:
    """验证 role 状态"""
    cfg = _get_config(ctx)
    role_id = state.get("role_id", cfg["role_id"])
    
    # 等待一小段时间让 role 启动
    time.sleep(2)
    
    return worker_probe.check_role_status(ctx, role_id, cfg["datainjector_container"])

def _probe_send_task(ctx: RunContext, state: Dict) -> ProbeResult:
    """发送任务到 Kafka"""
    cfg = _get_config(ctx)
    state["started_at"] = time.time()
    state["output_dir"] = cfg["output_dir"]
    from automation.ops.role.task import send_task_programmatic

    try:
        payload, method = send_task_programmatic(cfg["role_id"], run_id=ctx.run_id, topic=cfg["topic"])
    except Exception as exc:
        return ProbeResult(status=ProbeStatus.FAIL, detail=f"send task failed: {exc}")

    task_id = payload.get("task_id") or payload.get("taskId")
    state["task_id"] = task_id
    return ProbeResult(
        status=ProbeStatus.SUCCESS,
        detail=f"message sent via {method}",
        metrics={"topic": cfg["topic"], "task_id": task_id},
        payload={"message": payload},
    )


def _probe_consumer_lag(ctx: RunContext, state: Dict) -> ProbeResult:
    """检查消费者 lag（任务是否被消费）"""
    cfg = _get_config(ctx)
    
    # 等待一小段时间让消息被消费
    time.sleep(3)
    
    return worker_probe.check_kafka_consumer_lag(
        ctx,
        topic=cfg["topic"],
        group_id=cfg["group_id"],
        container=cfg["datainjector_container"],
        kafka_broker=cfg["kafka_broker"]
    )


def _probe_worker_logs(ctx: RunContext, state: Dict) -> ProbeResult:
    """检查 worker 结构化事件"""
    cfg = _get_config(ctx)
    role_id = state.get("role_id", cfg["role_id"])
    
    # 等待一段时间让 worker 处理
    time.sleep(5)

    required_events = [
        "emitter.fire",
        "caller.request",
        "caller.response",
        "pipeline.finish",
    ]
    optional_events = ["role.start", "role.stop"]

    return worker_probe.check_worker_events(
        ctx,
        required_events=required_events,
        optional_events=optional_events,
        run_id=ctx.run_id,
        role_id=role_id,
        container=cfg["datainjector_container"],
        since_seconds=45,
    )


def _probe_worker_errors(ctx: RunContext, state: Dict) -> ProbeResult:
    """检查 worker 错误事件"""
    cfg = _get_config(ctx)
    role_id = state.get("role_id", cfg["role_id"])

    error_events = [
        "caller.error",
        "pipeline.error",
        "queue.enqueue",
        "handler.error",
        "sink.error",
        "ws.read.error",
        "ws.heartbeat.error",
        "ws.subscribe.retry_error",
        "ws.subscribe.parse_error",
        "ws.subscribe.build_error",
        "ws.message.process_error",
        "ws.subscribe.ack_parse_error",
        "ws.heartbeat.payload_error",
        "ws.init.connect_error",
    ]

    return worker_probe.check_worker_errors(
        ctx,
        run_id=ctx.run_id,
        role_id=role_id,
        container=cfg["datainjector_container"],
        since_seconds=60,
        error_events=error_events,
    )


def _probe_verify_files(ctx: RunContext, state: Dict) -> ProbeResult:
    """验证文件生成"""
    cfg = _get_config(ctx)
    output_dir = state.get("output_dir") or cfg["output_dir"]
    started_at = state.get("started_at")
    wait_timeout = int(cfg.get("wait_timeout", 120))
    wait_interval = int(cfg.get("wait_interval", 5))
    container = cfg["datainjector_container"]

    return file_probe.verify_new_file(
        ctx,
        output_dir=output_dir,
        container=container,
        started_at=started_at,
        wait_timeout=wait_timeout,
        wait_interval=wait_interval,
        pattern="*.json",
    )


def _probe_cleanup(ctx: RunContext, state: Dict) -> ProbeResult:
    """清理 roles"""
    cfg = _get_config(ctx)
    role_ids = state.get("role_ids", [])
    
    return datainjector.stop_roles_by_ids(
        ctx=ctx,
        role_ids=role_ids,
        api=cfg.get("datainjector_api"),
        container=cfg["datainjector_container"],
        token=cfg.get("datainjector_token"),
    )


def build_scenario() -> Scenario:
    """构建测试场景：Binance LINK K线批量拉取"""
    state: Dict = {}
    
    return Scenario(
        name="binance_spot_link_kline_batch",
        tags=["type:integration", "module:file_sink", "datasource:binance"],
        stages=[
            # Stage 1: 前置检查
            Stage(
                name="precondition",
                probes=[
                    ProbeCall("worker.health", _probe_worker_health),
                    ProbeCall("api.connectivity", _probe_api_connectivity),
                    ProbeCall("file.permissions", _probe_file_permissions),
                ],
                tags=["layer:infra"],
            ),
            # Stage 2: 应用配置
            Stage(
                name="apply_roles",
                probes=[
                    ProbeCall("ingress.apply_roles", lambda ctx: _probe_apply_role(ctx, state)),
                    ProbeCall("ingress.role_status", lambda ctx: _probe_role_status(ctx, state)),
                ],
                tags=["layer:ingress"],
            ),
            # Stage 3: 提交任务
            Stage(
                name="submit",
                probes=[
                    ProbeCall("kafka.send_task", lambda ctx: _probe_send_task(ctx, state)),
                ],
                tags=["layer:ingress"],
            ),
            # Stage 4: 观测处理
            Stage(
                name="observe",
                probes=[
                    ProbeCall("kafka.consumer_lag", lambda ctx: _probe_consumer_lag(ctx, state)),
                    ProbeCall("worker.events", lambda ctx: _probe_worker_logs(ctx, state)),
                    ProbeCall("worker.errors", lambda ctx: _probe_worker_errors(ctx, state)),
                ],
                tags=["layer:flow"],
            ),
            # Stage 5: 验证结果
            Stage(
                name="verify",
                probes=[
                    ProbeCall("file.verify_output", lambda ctx: _probe_verify_files(ctx, state)),
                ],
                tags=["layer:verify"],
            ),
            # Stage 6: 清理
            Stage(
                name="cleanup",
                probes=[
                    ProbeCall("cleanup.stop", lambda ctx: _probe_cleanup(ctx, state)),
                ],
                tags=["layer:cleanup"],
            ),
        ],
    )
