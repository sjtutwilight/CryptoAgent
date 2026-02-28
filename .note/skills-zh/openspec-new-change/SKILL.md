---
name: openspec-new-change
description: 使用实验性的工件驱动工作流启动一个新的 OpenSpec 变更。当用户想以结构化、分步骤方式创建新功能、修复或修改时使用。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

使用实验性的工件驱动方式启动一个新变更。

**输入**：用户请求应包含一个变更名称（kebab-case），或者包含他们想构建内容的描述。

**步骤**

1. **如果没有明确输入，先询问要构建什么**

   使用 **AskUserQuestion 工具**（开放式问题，无预设选项）提问：
   > "你想处理什么变更？请描述你想构建或修复的内容。"

   根据用户描述推导 kebab-case 名称（例如："add user authentication" → `add-user-auth`）。

   **重要**：在没有理解用户要构建什么之前，不要继续。

2. **确定工作流 schema**

   除非用户明确要求其他工作流，否则使用默认 schema（即不传 `--schema`）。

   **仅在以下情况使用非默认 schema：**
   - 用户提到了具体 schema 名称 → 使用 `--schema <name>`
   - 用户说了 "show workflows" 或 "what workflows" → 运行 `openspec schemas --json` 并让用户选择

   **否则**：省略 `--schema`，使用默认值。

3. **创建变更目录**
   ```bash
   openspec new change "<name>"
   ```
   仅当用户请求了特定工作流时才添加 `--schema <name>`。
   该命令会在 `openspec/changes/<name>/` 下创建所选 schema 的脚手架目录。

4. **展示工件状态**
   ```bash
   openspec status --change "<name>"
   ```
   这会显示哪些工件需要创建，以及哪些工件已满足依赖并处于 ready 状态。

5. **获取首个工件的指令**
   第一个工件取决于 schema（例如 spec-driven 常见为 `proposal`）。
   检查 status 输出，找到第一个状态为 "ready" 的工件。
   ```bash
   openspec instructions <first-artifact-id> --change "<name>"
   ```
   该命令会输出创建首个工件所需的模板和上下文。

6. **停止并等待用户下一步指令**

**输出**

完成以上步骤后，总结：
- 变更名称与路径
- 使用的 schema/workflow 及其工件顺序
- 当前状态（0/N 个工件完成）
- 首个工件的模板
- 提示语："准备创建第一个工件吗？你只要描述这个变更要做什么，我就可以起草；或者直接让我继续。"

**护栏**
- 不要创建任何工件内容，只展示指令
- 不要推进到“展示第一个工件模板”之后
- 如果名称不合法（非 kebab-case），先让用户提供合法名称
- 如果同名变更已存在，建议继续该变更
- 使用非默认工作流时，要传 `--schema`
