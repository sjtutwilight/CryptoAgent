---
name: openspec-continue-change
description: 通过创建下一个工件继续推进 OpenSpec 变更。当用户希望推进变更、创建下一个工件或继续流程时使用。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

通过创建下一个工件继续推进一个 change。

**输入**：可选指定 change 名称。若省略，检查能否从对话上下文推断。若含糊或有歧义，你必须提示可用 change。

**步骤**

1. **若未提供 change 名称，提示用户选择**

   运行 `openspec list --json` 获取按最近修改时间排序的可用 change。然后用 **AskUserQuestion 工具**让用户选择要继续哪个 change。

   展示最近修改的前 3-4 个 change，包含：
   - Change 名称
   - Schema（若有 `schema` 字段则使用，否则显示 "spec-driven"）
   - 状态（例如 "0/5 tasks"、"complete"、"no tasks"）
   - 最近修改时间（`lastModified`）

   将最近修改的 change 标记为 "(Recommended)"，因为它最可能是用户想继续的项。

   **重要**：不要猜测或自动选择 change。必须让用户选择。

2. **检查当前状态**
   ```bash
   openspec status --change "<name>" --json
   ```
   解析 JSON 了解当前状态。返回内容包括：
   - `schemaName`：当前工作流 schema（例如 "spec-driven"）
   - `artifacts`：工件数组及状态（"done"、"ready"、"blocked"）
   - `isComplete`：是否所有工件均已完成

3. **根据状态执行**：

   ---

   **若全部工件完成（`isComplete: true`）**：
   - 祝贺用户
   - 展示最终状态（含 schema）
   - 建议："All artifacts created! You can now implement this change or archive it."
   - 停止

   ---

   **若存在可创建工件**（状态里有 `status: "ready"`）：
   - 从状态输出中选取第一个 `status: "ready"` 的工件
   - 获取该工件指令：
     ```bash
     openspec instructions <artifact-id> --change "<name>" --json
     ```
   - 解析 JSON。关键字段：
     - `context`：项目背景（给你用的约束，不要写进输出）
     - `rules`：工件规则（给你用的约束，不要写进输出）
     - `template`：输出文件结构模板
     - `instruction`：schema 级指导
     - `outputPath`：写入路径
     - `dependencies`：需要先读取的已完成工件
   - **创建工件文件**：
     - 读取已完成依赖工件作为上下文
     - 使用 `template` 结构并填充内容
     - 写作时应用 `context` 与 `rules` 约束，但不要将其原样写入文件
     - 写入指令指定路径
   - 展示已创建内容和新解锁的后续工件
   - 创建一个工件后即停止

   ---

   **若没有 ready 工件（全部 blocked）**：
   - 这在有效 schema 下不应发生
   - 展示状态并建议排查问题

4. **创建工件后展示进度**
   ```bash
   openspec status --change "<name>"
   ```

**输出**

每次调用后展示：
- 创建了哪个工件
- 当前使用的 schema 工作流
- 当前进度（N/M 完成）
- 已解锁哪些后续工件
- 提示语："Want to continue? Just ask me to continue or tell me what to do next."

**工件创建指南**

工件类型及其用途取决于 schema。使用 instructions 输出中的 `instruction` 字段来理解要创建什么。

常见工件模式：

**spec-driven schema**（proposal → specs → design → tasks）：
- **proposal.md**：若变更不清晰先询问用户。填写 Why、What Changes、Capabilities、Impact。
  - Capabilities 小节至关重要：列出的每个 capability 都需要对应 spec 文件。
- **specs/<capability>/spec.md**：每个 capability 一个 spec（使用 capability 名称，不用 change 名称）。
- **design.md**：记录技术决策、架构与实现方法。
- **tasks.md**：将实现拆分为带复选框任务。

对于其他 schema，按 CLI 输出中的 `instruction` 执行。

**护栏**
- 每次调用只创建 **一个** 工件
- 创建前始终读取依赖工件
- 不跳过工件，不乱序创建
- 上下文不清晰时先问用户
- 写入后确认工件文件存在，再更新进度
- 遵循 schema 工件顺序，不要假定固定文件名
- **重要**：`context` 与 `rules` 是给你的约束，不是文件内容
  - 不要把 `<context>`、`<rules>`、`<project_context>` 区块复制到工件里
  - 这些用于指导写作，不应直接出现在输出中
