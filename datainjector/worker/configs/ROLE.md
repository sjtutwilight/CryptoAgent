


- url拼接:datasource的base_url+role的path_template
- 任务统一通过kafka batch.tasks topic下发，当前的poll变成topic内指定，去掉single合并到kafka_command
- 数据源定位为数据供应商的协议粒度，如dune.sim.rest。 binance.ws
```java
  
  
  - role_id: "token-holders"
    datasource_id: "dune.sim"
    data_form:'page'|'stream'|'single'# page 分页拉取全量，stream 流式数据， single 简单单次拉取
    role_param:"chain_id","address" # role开启时传入的参数. 若传入 url_config中参数，可覆盖默认值如page_size: 1000
    url_config:
	    path_template: "/v1/evm/token-holders/{chain_id}/{address}"
	    method:"GET"|'post'
	    #page类型参数
      page_size: 500
      page_request: "offset"
      page_response: "next_offset"
    sink_config:
	    type:"localfile"
      data_field: "holders"
      output_dir: "/tmp/dune/token-holders/{chain_id}/{address}"
      output_format: "json"
      filename_prefix: "holders"
      max_records_per_file: 10000
		  extends_config:# 为特殊任务准备，非必要不添加
		  manifest:#简单的元数据配置，file sink能够为文件生成轻量元数据文件支持后续数据完整性即可

```

```java

datasources:
	-id: "dune.sim"
    protocol: "http"
    auth: # 参数可基于具体厂商定制化。
      type: "api_key_env"
      header: "X-Sim-Api-Key"
      api_key_env: "DUNE_SIM_API_KEY"
    rate_limit:# 按数据源维度统一一个安全值
    doc_url:数据源官方文档的url，找不到可控
```