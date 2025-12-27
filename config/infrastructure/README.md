# 基础设施配置入口

所有容器、端口以及凭证相关的可变参数统一维护在该目录，避免散落在 `automation/orchestration/docker-compose.yml`、脚本或 Dockerfile 中。

## 目录结构

```
config/infrastructure
├── env/
│   ├── docker.env                # 默认（示例）环境，仓库内提供的可读配置
│   ├── docker.local.env          # 本地覆盖（加入 .gitignore，需要开发者自行复制）
│   └── README.md                 # （可选）额外说明
├── schema/
│   └── docker-env.schema.json    # env 文件字段校验 Schema
└── README.md                     # 当前文件
```

## 使用方式

1. 复制 `config/infrastructure/env/docker.env` 为 `docker.local.env` 并按需调整。  
2. 运行脚本或 docker compose 前设置 `INFRA_ENV_FILE=config/infrastructure/env/docker.local.env`，否则默认读取 `docker.env`。  
3. 容器启动命令推荐：

```bash
docker compose -f automation/orchestration/docker-compose.yml --env-file config/infrastructure/env/docker.local.env up -d
# 或者
INFRA_ENV_FILE=config/infrastructure/env/docker.local.env ./automation/infra/load-infra-env.sh
```

## 约定

- **禁止**在脚本/Dockerfile 中写死端口、用户名或容器名称，统一改为读取 env。
- 需要新增配置项时先更新 `config/infrastructure/env/docker.env` 与 `schema/docker-env.schema.json`，再修改依赖脚本。
- 使用 `automation/infra/validate-config.sh` 校验 env 是否满足 Schema 要求，可在 CI 中执行。

## 资源配置

所有容器的 CPU / 内存限制集中维护在 `env/docker.env` 中（变量名以 `*_CPUS`、`*_MEM` 结尾）。调整资源只需修改该文件后重新启动 compose，无需在 `docker-compose.yml` 中逐项修改。
