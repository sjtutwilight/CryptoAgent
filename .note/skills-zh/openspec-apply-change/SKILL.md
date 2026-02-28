---
name: openspec-apply-change
description: 根据 OpenSpec 变更执行任务实现。当用户希望开始实现、继续实现或逐步完成任务时使用。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

根据 OpenSpec change 执行任务实现。

**输入**：可选指定 change 名称。若省略，检查是否可从对话上下文推断。若含糊或有歧义，你必须提示可用 change。

**步骤**

1. **选择 change**

   若提供了名称，则直接使用。否则：
   - 若用户在对话中提到 change，则从上下文推断
   - 若仅有一个 active change，则自动选择
   - 若有歧义，运行 `openspec list --json` 获取可用 change，并使用 **AskUserQuestion 工具**让用户选择

   始终要明确告知："Using change: <name>"，并说明如何覆盖（例如 `/opsx:apply <other>`）。

2. **检查状态以理解 schema**
   ```bash
   openspec status --change "<name>" --json
   ```
   解析 JSON 了解：
   - `schemaName`：当前工作流（例如 "spec-driven"）
   - 哪个工件包含任务（spec-driven 通常是 `tasks`，其他以 status 为准）

3. **获取 apply 指令**

   ```bash
   openspec instructions apply --change "<name>" --json
   ```

   返回内容包括：
   - 上下文文件路径（取决于 schema，可能是 proposal/specs/design/tasks 或 spec/tests/implementation/docs）
   - 进度（总数、完成、剩余）
   - 带状态的任务列表
   - 基于当前状态的动态指令

   **状态处理：**
   - 若 `state: "blocked"`（缺少工件）：展示提示并建议使用 openspec-continue-change
   - 若 `state: "all_done"`：祝贺并建议归档
   - 否则：继续实现

4. **读取上下文文件**

   读取 apply 指令输出中的 `contextFiles` 列表。
   文件取决于当前 schema：
   - **spec-driven**：proposal、specs、design、tasks
   - 其他 schema：按 CLI 输出的 contextFiles 执行

5. **展示当前进度**

   展示：
   - 正在使用的 schema
   - 进度："N/M tasks complete"
   - 剩余任务概览
   - CLI 给出的动态指令

6. **执行任务（循环直到完成或被阻塞）**

   对每个待办任务：
   - 展示当前正在处理哪一项
   - 做所需代码改动
   - 保持改动最小且聚焦
   - 在任务文件中标记完成：`- [ ]` → `- [x]`
   - 继续下一项

   **以下情况需要暂停：**
   - 任务不清晰 → 询问澄清
   - 实现暴露设计问题 → 建议更新工件
   - 遇到错误或阻塞 → 报告并等待指导
   - 用户中断

7. **完成或暂停时展示状态**

   展示：
   - 本次会话完成了哪些任务
   - 总体进度："N/M tasks complete"
   - 若全部完成：建议归档
   - 若暂停：说明原因并等待下一步指示

**实现过程输出示例**

```
## Implementing: <change-name> (schema: <schema-name>)

Working on task 3/7: <task description>
[...implementation happening...]
✓ Task complete

Working on task 4/7: <task description>
[...implementation happening...]
✓ Task complete
```

**完成输出示例**

```
## Implementation Complete

**Change:** <change-name>
**Schema:** <schema-name>
**Progress:** 7/7 tasks complete ✓

### Completed This Session
- [x] Task 1
- [x] Task 2
...

All tasks complete! Ready to archive this change.
```

**暂停输出示例（遇到问题）**

```
## Implementation Paused

**Change:** <change-name>
**Schema:** <schema-name>
**Progress:** 4/7 tasks complete

### Issue Encountered
<description of the issue>

**Options:**
1. <option 1>
2. <option 2>
3. Other approach

What would you like to do?
```

**护栏**
- 持续推进任务，直到完成或被阻塞
- 开始前必须读取上下文文件（来自 apply 指令输出）
- 任务有歧义时，先暂停并询问，再实现
- 实现暴露问题时，先暂停并建议更新工件
- 代码改动保持最小且按任务范围收敛
- 每完成一项任务，立刻更新复选框
- 遇到错误、阻塞或需求不清晰时必须暂停，不要猜
- 使用 CLI 返回的 contextFiles，不要假设固定文件名

**流式工作流集成**

该 skill 支持 “actions on a change” 模型：

- **可随时调用**：即使工件尚未全完成（只要任务存在），或已部分实现，或与其他动作交错进行
- **允许更新工件**：实现中若暴露设计问题，可建议更新工件；不受固定阶段锁定，流程是流动的
