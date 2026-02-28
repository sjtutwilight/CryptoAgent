---
name: openspec-ff-change
description: 快速推进 OpenSpec 工件创建。当用户希望不逐步停顿、而是一次性创建实现所需全部工件时使用。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

快速推进工件创建：一次性生成启动实现所需的全部内容。

**输入**：用户请求应包含一个变更名称（kebab-case），或者包含他们想构建内容的描述。

**步骤**

1. **如果没有明确输入，先询问要构建什么**

   使用 **AskUserQuestion 工具**（开放式问题，无预设选项）提问：
   > "你想处理什么变更？请描述你想构建或修复的内容。"

   根据用户描述推导 kebab-case 名称（例如："add user authentication" → `add-user-auth`）。

   **重要**：在没有理解用户要构建什么之前，不要继续。

2. **创建变更目录**
   ```bash
   openspec new change "<name>"
   ```
   该命令会在 `openspec/changes/<name>/` 下创建脚手架目录。

3. **获取工件构建顺序**
   ```bash
   openspec status --change "<name>" --json
   ```
   解析 JSON 获取：
   - `applyRequires`：实现前所需工件 ID 数组（例如 ` ["tasks"] `）
   - `artifacts`：所有工件及其状态与依赖

4. **按顺序创建工件，直到可执行 apply**

   使用 **TodoWrite 工具**跟踪工件创建进度。

   按依赖顺序循环处理（优先处理没有待满足依赖的工件）：

   a. **对每个状态为 `ready` 的工件（依赖已满足）**：
      - 获取指令：
        ```bash
        openspec instructions <artifact-id> --change "<name>" --json
        ```
      - 指令 JSON 包含：
        - `context`：项目背景（给你用的约束，不要写进输出）
        - `rules`：工件规则（给你用的约束，不要写进输出）
        - `template`：输出文件结构模板
        - `instruction`：该工件类型的 schema 级指导
        - `outputPath`：输出路径
        - `dependencies`：需要先读取的已完成工件
      - 读取已完成依赖工件作为上下文
      - 使用 `template` 结构创建工件文件
      - 写作时遵循 `context` 与 `rules`，但不要把它们原样复制到文件中
      - 简要显示进度："✓ Created <artifact-id>"

   b. **持续直到 `applyRequires` 中的工件全部完成**：
      - 每创建一个工件后，重新运行 `openspec status --change "<name>" --json`
      - 检查 `applyRequires` 中每个工件 ID 在 artifacts 数组里是否都为 `status: "done"`
      - 全部完成后停止

   c. **如果工件需要用户输入**（上下文不清晰）：
      - 使用 **AskUserQuestion 工具**澄清
      - 然后继续创建

5. **展示最终状态**
   ```bash
   openspec status --change "<name>"
   ```

**输出**

全部工件完成后，总结：
- 变更名称与路径
- 已创建工件列表及简述
- 当前可执行状态："All artifacts created! Ready for implementation."
- 提示语："运行 `/opsx:apply` 或直接让我实现，开始处理任务。"

**工件创建指南**

- 每种工件都遵循 `openspec instructions` 的 `instruction` 字段
- schema 决定工件应包含什么，按其定义执行
- 创建新工件前先读依赖工件
- 使用 `template` 作为输出结构，并填充各小节
- **重要**：`context` 与 `rules` 是给你的约束，不是文件内容
  - 不要把 `<context>`、`<rules>`、`<project_context>` 区块复制进工件
  - 它们用于指导写作，不应直接出现在输出里

**护栏**
- 必须创建实现所需的全部工件（以 schema 的 `apply.requires` 为准）
- 创建新工件前必须读取依赖工件
- 如果上下文关键性不清晰，询问用户；但应尽量做合理决策保持推进
- 如果同名变更已存在，建议继续已有变更
- 写入后要确认工件文件确实存在，再继续下一步
