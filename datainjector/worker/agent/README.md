### 主要模块

- role：一类任务的执行者与协调者。启动时基于配置初始化各模块。
- resource: 资源，如http连接池、websocket连接数管理、限流token。
- emitter: 任务触发器，role监听emitter channel来执行任务生命周期。包括轮询与命令式两种。
    - 轮询：基于polling配置进行触发，如轮询间隔，轮询url、参数等。
    - 命令式：监听kafka消息来获取http任务。
    - 一次性：websocket订阅直接从配置中获取，不需要监听与轮询。
- caller：role接收任务后获取resource，交给caller来执行数据源调用。caller主要包括native_call与sdk_call两种
    - native_call：如调用http,websocket。caller基于任务中携带的参数进行简单组装后调用底层协议。http为单次调用，websocket则为发起订阅/退订
    - sdk_call：调用sdk。如通过go-ethereum sdk来调用本地hardhat node。每种任务需要有单独的类来组织sdk调用，如获取区块、查询某token balance分布。
- queue:role在获取到caller返回时将其塞入队列。从而将后续的数据处理与请求隔离。

### 工作流

role监听emitter获取任务，基于任务获取相应资源，若获取不到资源则抛异常。获取到则通过caller进行底层调用。接收caller返回后送入queue.

### 配置

- 若为空或没提到的配置项则代表不需要。如没有resource则代表不需要获取任何资源即可执行后续
- caller_class对应具体类名，将特定开发逻辑放入类中。

```yaml
roles:
	- role_id: "localnode-block"
        emitter: "polling"
        polling_interval: 2  # 秒
        caller: "sdk_call"
        caller_class: "LocalGetBlock"
        caller_params:
            rpc_endpoint: "http://localhost:8545"
            chain_id: "local"
            confirmations: 0
            max_blocks_per_poll: 5
            block_delay: 0
        handlers:
            - type: "dex_parser"
            with:
                chain_id: "local"
        sink:
            type: "kafka"
            with:
            brokers:
            - "localhost:9092"
            topic: "dex_transaction"
            key_from: ["chain_id", "tx_hash"]
        queue: { size: 5000 }

    - role_id: "mockprovider-websocket"
        emitter: "single"
        caller: "native_call"
        protocol_config:
            protocol: "websocket"
            url: "ws://localhost:8090/ws"
            params:
                subscribe: "newHeads"
                heartbeat_ms: 30000
                reconnect: { backoff_base_seconds: 2, backoff_max_seconds: 60 }
        queue: { size: 5000 }
        sink:
            type: "kafka"
            with:
                brokers:
                - "localhost:9092"
                topic: "chain.ethereum.blocks"
                key_from: ["chain_id","block_number"]
        queue: { size: 5000 }
```

