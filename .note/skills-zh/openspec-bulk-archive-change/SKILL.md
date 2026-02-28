---
name: openspec-bulk-archive-change
description: 一次归档多个已完成变更。当需要并行归档多个 change 时使用。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

在一次操作中归档多个已完成变更。

该 skill 支持批量归档 change，并通过检查代码库来判断“实际实现了什么”，从而智能处理 spec 冲突。

**输入**：无需输入（会提示用户选择）

**步骤**

1. **获取 active changes**

   运行 `openspec list --json` 获取所有 active change。

   若不存在 active change，告知用户并停止。

2. **提示用户选择 changes**

   使用 **AskUserQuestion 工具**（多选）让用户选择要归档的 changes：
   - 展示每个 change 及其 schema
   - 包含 "All changes" 选项
   - 允许任意数量选择（1+ 可用，2+ 是典型场景）

   **重要**：不要自动选择，必须让用户选择。

3. **批量校验：收集所有已选 change 的状态**

   对每个选中的 change，收集：

   a. **工件状态** - 运行 `openspec status --change "<name>" --json`
      - 解析 `schemaName` 与 `artifacts`
      - 记录哪些工件是 `done`，哪些不是

   b. **任务完成度** - 读取 `openspec/changes/<name>/tasks.md`
      - 统计 `- [ ]`（未完成）与 `- [x]`（已完成）
      - 若不存在 tasks 文件，记为 "No tasks"

   c. **Delta specs** - 检查 `openspec/changes/<name>/specs/` 目录
      - 列出涉及的 capability specs
      - 对每个 spec 提取 requirement 名称（匹配 `### Requirement: <name>`）

4. **检测 spec 冲突**

   构建 `capability -> [触达该 capability 的 changes]` 映射：

   ```
   auth -> [change-a, change-b]  <- CONFLICT (2+ changes)
   api  -> [change-c]            <- OK (only 1 change)
   ```

   当同一 capability 被 2 个及以上已选 change 触达时，判定为冲突。

5. **以 agent 方式解决冲突**

   **对每个冲突**，调查代码库：

   a. **读取各冲突 change 的 delta specs**，理解各自要新增/修改什么

   b. **在代码库中搜索实现证据**：
      - 查找实现了各 delta spec requirement 的代码
      - 检查相关文件、函数或测试

   c. **确定解决策略**：
      - 若只有一个 change 实际已实现 -> 仅同步该 change 的 specs
      - 若两个都实现了 -> 按时间顺序应用（旧的先，新的后覆盖）
      - 若都未实现 -> 跳过 spec 同步，并警告用户

   d. **记录每个冲突的决议**：
      - 同步哪个 change 的 specs
      - 同步顺序（若两者都同步）
      - 决策依据（在代码库里发现了什么）

6. **展示合并状态表**

   展示汇总表：

   ```
   | Change               | Artifacts | Tasks | Specs   | Conflicts | Status |
   |---------------------|-----------|-------|---------|-----------|--------|
   | schema-management   | Done      | 5/5   | 2 delta | None      | Ready  |
   | project-config      | Done      | 3/3   | 1 delta | None      | Ready  |
   | add-oauth           | Done      | 4/4   | 1 delta | auth (!)  | Ready* |
   | add-verify-skill    | 1 left    | 2/5   | None    | None      | Warn   |
   ```

   对冲突展示解决方案：
   ```
   * Conflict resolution:
     - auth spec: Will apply add-oauth then add-jwt (both implemented, chronological order)
   ```

   对未完成项展示警告：
   ```
   Warnings:
   - add-verify-skill: 1 incomplete artifact, 3 incomplete tasks
   ```

7. **确认批量操作**

   使用 **AskUserQuestion 工具**进行一次确认：

   - "Archive N changes?"，选项根据状态动态给出
   - 可能包括：
     - "Archive all N changes"
     - "Archive only N ready changes (skip incomplete)"
     - "Cancel"

   若含未完成项，要明确提示“归档将带警告继续”。

8. **对每个确认后的 change 执行归档**

   按既定顺序处理 changes（遵守冲突解决顺序）：

   a. **同步 specs**（若存在 delta specs）：
      - 采用 openspec-sync-specs 的方式（agent 驱动智能合并）
      - 有冲突时按已决议顺序应用
      - 记录是否完成同步

   b. **执行归档**：
      ```bash
      mkdir -p openspec/changes/archive
      mv openspec/changes/<name> openspec/changes/archive/YYYY-MM-DD-<name>
      ```

   c. **记录每个 change 的结果**：
      - Success：归档成功
      - Failed：归档失败（记录错误）
      - Skipped：按用户选择跳过（如适用）

9. **展示总结**

   展示最终结果：

   ```
   ## Bulk Archive Complete

   Archived 3 changes:
   - schema-management-cli -> archive/2026-01-19-schema-management-cli/
   - project-config -> archive/2026-01-19-project-config/
   - add-oauth -> archive/2026-01-19-add-oauth/

   Skipped 1 change:
   - add-verify-skill (user chose not to archive incomplete)

   Spec sync summary:
   - 4 delta specs synced to main specs
   - 1 conflict resolved (auth: applied both in chronological order)
   ```

   若有失败项：
   ```
   Failed 1 change:
   - some-change: Archive directory already exists
   ```

**冲突解决示例**

示例 1：仅一个已实现
```
Conflict: specs/auth/spec.md touched by [add-oauth, add-jwt]

Checking add-oauth:
- Delta adds "OAuth Provider Integration" requirement
- Searching codebase... found src/auth/oauth.ts implementing OAuth flow

Checking add-jwt:
- Delta adds "JWT Token Handling" requirement
- Searching codebase... no JWT implementation found

Resolution: Only add-oauth is implemented. Will sync add-oauth specs only.
```

示例 2：两个都已实现
```
Conflict: specs/api/spec.md touched by [add-rest-api, add-graphql]

Checking add-rest-api (created 2026-01-10):
- Delta adds "REST Endpoints" requirement
- Searching codebase... found src/api/rest.ts

Checking add-graphql (created 2026-01-15):
- Delta adds "GraphQL Schema" requirement
- Searching codebase... found src/api/graphql.ts

Resolution: Both implemented. Will apply add-rest-api specs first,
then add-graphql specs (chronological order, newer takes precedence).
```

**成功输出示例**

```
## Bulk Archive Complete

Archived N changes:
- <change-1> -> archive/YYYY-MM-DD-<change-1>/
- <change-2> -> archive/YYYY-MM-DD-<change-2>/

Spec sync summary:
- N delta specs synced to main specs
- No conflicts (or: M conflicts resolved)
```

**部分成功输出示例**

```
## Bulk Archive Complete (partial)

Archived N changes:
- <change-1> -> archive/YYYY-MM-DD-<change-1>/

Skipped M changes:
- <change-2> (user chose not to archive incomplete)

Failed K changes:
- <change-3>: Archive directory already exists
```

**无可归档项输出示例**

```
## No Changes to Archive

No active changes found. Use `/opsx:new` to create a new change.
```

**护栏**
- 允许任意数量选择（1+ 可用，2+ 常见）
- 始终提示选择，不自动选
- 尽早检测 spec 冲突，并通过代码库证据解决
- 若两个 change 都已实现，按时间顺序应用 specs
- 仅在缺失实现证据时跳过 spec 同步（并警告用户）
- 确认前要清晰展示每个 change 状态
- 整批仅做一次确认
- 跟踪并汇报所有结果（success/skip/fail）
- 移动归档时保留 `.openspec.yaml`
- 归档目标目录使用当天日期：`YYYY-MM-DD-<name>`
- 若目标已存在，该 change 失败但应继续处理其他 change
