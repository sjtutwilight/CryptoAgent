---
name: openspec-archive-change
description: 在实验性工作流中归档已完成的变更。当用户希望在实现完成后做收尾并归档时使用。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

在实验性工作流中归档一个已完成变更。

**输入**：可选指定一个 change 名称。若省略，检查能否从对话上下文推断。若含糊或有歧义，你必须提示可用 change 让用户选择。

**步骤**

1. **若未提供 change 名称，提示用户选择**

   运行 `openspec list --json` 获取可用 change。使用 **AskUserQuestion 工具**让用户选择。

   仅展示 active change（未归档）。
   如可用，展示每个 change 使用的 schema。

   **重要**：不要猜测或自动选择 change。必须由用户选择。

2. **检查工件完成状态**

   运行 `openspec status --change "<name>" --json` 检查工件完成度。

   解析 JSON 了解：
   - `schemaName`：正在使用的工作流
   - `artifacts`：工件列表及其状态（`done` 或其他）

   **如果有任何工件不是 `done`：**
   - 显示警告并列出未完成工件
   - 使用 **AskUserQuestion 工具**确认用户是否仍要继续
   - 用户确认则继续

3. **检查任务完成状态**

   读取任务文件（通常是 `tasks.md`）检查是否有未完成任务。

   统计 `- [ ]`（未完成）与 `- [x]`（已完成）。

   **如果发现未完成任务：**
   - 显示警告并给出未完成任务数量
   - 使用 **AskUserQuestion 工具**确认用户是否仍要继续
   - 用户确认则继续

   **如果不存在任务文件：**跳过任务警告，直接继续。

4. **评估 delta spec 的同步状态**

   检查 `openspec/changes/<name>/specs/` 下是否有 delta specs。若没有，跳过同步提示直接继续。

   **如果存在 delta specs：**
   - 将每个 delta spec 与对应主 spec（`openspec/specs/<capability>/spec.md`）比较
   - 判断将应用哪些变更（新增、修改、删除、重命名）
   - 在提示前先展示一份合并摘要

   **提示选项：**
   - 若需要变更："Sync now (recommended)", "Archive without syncing"
   - 若已同步："Archive now", "Sync anyway", "Cancel"

   若用户选择同步，执行 `/opsx:sync` 逻辑（使用 `openspec-sync-specs` skill）。无论是否同步，都可继续归档。

5. **执行归档**

   若归档目录不存在则创建：
   ```bash
   mkdir -p openspec/changes/archive
   ```

   使用当前日期生成目标名：`YYYY-MM-DD-<change-name>`

   **检查目标是否已存在：**
   - 若存在：报错并建议重命名已有归档或改用其他日期
   - 若不存在：移动变更目录到归档

   ```bash
   mv openspec/changes/<name> openspec/changes/archive/YYYY-MM-DD-<name>
   ```

6. **展示总结**

   展示归档完成摘要，包括：
   - change 名称
   - 使用的 schema
   - 归档位置
   - 是否已同步 specs（如适用）
   - 警告信息（工件/任务未完成）

**成功输出示例**

```
## Archive Complete

**Change:** <change-name>
**Schema:** <schema-name>
**Archived to:** openspec/changes/archive/YYYY-MM-DD-<name>/
**Specs:** ✓ Synced to main specs (or "No delta specs" or "Sync skipped")

All artifacts complete. All tasks complete.
```

**护栏**
- 未提供 change 名称时，必须提示用户选择
- 用工件图（`openspec status --json`）检查完成状态
- 不要因警告而强制阻止归档：应提示并确认
- 移动归档时要保留 `.openspec.yaml`（随目录一起移动）
- 清晰展示发生了什么
- 若请求同步，采用 openspec-sync-specs 的方式（agent 驱动）
- 若存在 delta specs，提示前必须先完成同步评估并展示合并摘要
