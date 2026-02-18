# NF-01/NF-02/NF-03 灰度演练结果

- 演练日期：2026-02-16
- 执行脚本：`tools/nf01_nf03_gray_drill.sh`
- 结论：全部用例通过

## 用例结果

1. NF-01 queue finalization
- 命令：`go test ./internal/role -run 'TestQueuePipelineSinkFailureMustNotReportSuccess|TestFinalizeQueuedMessage' -count=1`
- 结果：PASS

2. NF-02 websocket bounded buffer
- 命令：`go test ./internal/caller -run 'TestBufferMessageDropOldest|TestBufferMessageDropNewest' -count=1`
- 结果：PASS

3. NF-03 backfill enqueue semantics
- 命令：`go test ./internal/handler/integrity -run 'TestSchedulerNoTargetReturnsError|TestChannelTargetTimeout|TestChannelTargetQueueFull|TestCompensationQueuePersistAndReplay|TestSchedulerDedupByKeyUntilResult|TestSequenceEnginePendingDedupAndMergedIntent|TestSequenceEngineIgnoresOutOfOrderResult|TestSequenceEngineCooldownRecovery' -count=1`
- 结果：PASS

## 备注

- 运行日志包含一条 locale warning（`LC_ALL`），不影响测试结果与功能验证。
- AAVE 四角色 30 分钟在线观测属于发布前演练步骤，需在目标环境按 `doc/nf01_nf03_gray_rollout.md` 的检查清单执行并回填结果。
