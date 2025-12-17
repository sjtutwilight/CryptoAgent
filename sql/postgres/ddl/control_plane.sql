-- ============================================================
-- 模块: 控制平面服务（Control Plane Service）
-- 存储: PostgreSQL
-- 维护: DataInjector/control-plane-service
-- 用途: 任务调度、状态管理、限流控制
-- ============================================================

-- 删除现有的tasks表（如果存在）
DROP TABLE IF EXISTS tasks CASCADE;

-- ========================================
-- 1. 任务表
-- ========================================
CREATE TABLE tasks (
    -- 主键ID（自增）
    id BIGSERIAL PRIMARY KEY,
    
    -- 任务唯一标识符
    task_id VARCHAR(64) UNIQUE NOT NULL,
    
    -- 数据源ID
    data_source_id VARCHAR(64) NOT NULL,
    
    -- 任务状态：PENDING, PROCESSING, SUCCESS, RETRY, FAILED
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    
    -- 重试次数
    retry_count INTEGER NOT NULL DEFAULT 0,
    
    -- 最大重试次数
    max_retry_count INTEGER NOT NULL DEFAULT 3,
    
    -- 任务类型（如：http_jsonrpc）
    task_type VARCHAR(64),
    
    -- 任务载荷（JSON格式，包含请求参数等）
    payload JSONB,
    
    -- 元数据（JSON格式，包含缺失区间信息、检测时间等）
    metadata JSONB,
    
    -- 计划执行时间
    scheduled_time TIMESTAMP NOT NULL,
    
    -- 实际开始时间
    started_at TIMESTAMP,
    
    -- 完成时间
    completed_at TIMESTAMP,
    
    -- HTTP状态码或响应码
    status_code INTEGER,
    
    -- 错误消息或响应消息
    message TEXT,
    
    -- 执行耗时（毫秒）
    duration_ms BIGINT,
    
    -- 数据大小（字节）
    data_size INTEGER,
    
    -- 成本权重（用于限流计算）
    cost INTEGER NOT NULL DEFAULT 1,
    
    -- 优先级（数值越小优先级越高）
    priority INTEGER NOT NULL DEFAULT 5,
    
    -- 创建时间
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- 更新时间
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- 2. 创建索引以提升查询性能
-- ========================================
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_data_source_id ON tasks(data_source_id);
CREATE INDEX idx_tasks_scheduled_time ON tasks(scheduled_time);
CREATE INDEX idx_tasks_created_at ON tasks(created_at);
CREATE INDEX idx_tasks_status_scheduled ON tasks(status, scheduled_time);
CREATE INDEX idx_tasks_priority_scheduled ON tasks(priority, scheduled_time);

-- 为JSONB字段创建GIN索引，提升JSON查询性能
CREATE INDEX idx_tasks_payload_gin ON tasks USING GIN (payload);
CREATE INDEX idx_tasks_metadata_gin ON tasks USING GIN (metadata);

-- ========================================
-- 3. 添加表注释
-- ========================================
COMMENT ON TABLE tasks IS '控制平面任务表 - 用于管理数据采集任务的调度和执行';
COMMENT ON COLUMN tasks.id IS '主键ID（自增）';
COMMENT ON COLUMN tasks.task_id IS '任务唯一标识符';
COMMENT ON COLUMN tasks.data_source_id IS '数据源ID';
COMMENT ON COLUMN tasks.status IS '任务状态：PENDING-待处理, PROCESSING-处理中, SUCCESS-成功, RETRY-待重试, FAILED-失败';
COMMENT ON COLUMN tasks.retry_count IS '当前重试次数';
COMMENT ON COLUMN tasks.max_retry_count IS '最大允许重试次数';
COMMENT ON COLUMN tasks.task_type IS '任务类型（如：http_jsonrpc）';
COMMENT ON COLUMN tasks.payload IS '任务载荷（JSON格式），包含请求URL、方法、参数等';
COMMENT ON COLUMN tasks.metadata IS '任务元数据（JSON格式），包含缺失区间起止、检测时间、序列信息等';
COMMENT ON COLUMN tasks.scheduled_time IS '计划执行时间';
COMMENT ON COLUMN tasks.started_at IS '实际开始执行时间';
COMMENT ON COLUMN tasks.completed_at IS '任务完成时间';
COMMENT ON COLUMN tasks.status_code IS 'HTTP状态码或响应码';
COMMENT ON COLUMN tasks.message IS '错误消息或响应消息';
COMMENT ON COLUMN tasks.duration_ms IS '任务执行耗时（毫秒）';
COMMENT ON COLUMN tasks.data_size IS '返回数据大小（字节）';
COMMENT ON COLUMN tasks.cost IS '成本权重（用于限流计算）';
COMMENT ON COLUMN tasks.priority IS '优先级（数值越小优先级越高）';
COMMENT ON COLUMN tasks.created_at IS '记录创建时间';
COMMENT ON COLUMN tasks.updated_at IS '记录更新时间';

-- 打印成功消息
SELECT 'Tasks表创建完成！' as message;

