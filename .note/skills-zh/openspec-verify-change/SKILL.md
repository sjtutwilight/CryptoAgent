---
name: openspec-verify-change
description: 验证实现是否与变更工件一致。当用户希望在归档前确认实现是否完整、正确且一致时使用。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

验证实现是否与 change 工件（specs、tasks、design）一致。

**输入**：可选指定 change 名称。若省略，检查能否从对话上下文推断。若含糊或有歧义，你必须提示可用 change。

**步骤**

1. **若未提供 change 名称，提示用户选择**

   运行 `openspec list --json` 获取可用 change。使用 **AskUserQuestion 工具**让用户选择。

   展示带实现任务的 change（存在 tasks 工件）。
   如可用，展示每个 change 的 schema。
   将有未完成任务的 change 标记为 "(In Progress)"。

   **重要**：不要猜测或自动选择 change。必须让用户选择。

2. **检查状态以理解 schema**
   ```bash
   openspec status --change "<name>" --json
   ```
   解析 JSON 了解：
   - `schemaName`：正在使用的工作流（例如 "spec-driven"）
   - 该 change 包含哪些工件

3. **获取 change 目录并加载工件**

   ```bash
   openspec instructions apply --change "<name>" --json
   ```

   该命令会返回 change 目录和上下文文件。读取 `contextFiles` 中全部可用工件。

4. **初始化验证报告结构**

   建立三维度报告结构：
   - **Completeness（完整性）**：跟踪任务与 spec 覆盖
   - **Correctness（正确性）**：跟踪 requirement 实现与 scenario 覆盖
   - **Coherence（一致性）**：跟踪设计遵循与模式一致性

   每个维度的问题级别可以是 CRITICAL、WARNING 或 SUGGESTION。

5. **验证 Completeness（完整性）**

   **任务完成度**：
   - 若 `contextFiles` 中存在 tasks.md，则读取
   - 解析复选框：`- [ ]`（未完成）与 `- [x]`（已完成）
   - 统计完成数与总数
   - 若存在未完成任务：
     - 对每个未完成任务加入一条 CRITICAL
     - 建议："Complete task: <description>" 或 "Mark as done if already implemented"

   **Spec 覆盖**：
   - 若 `openspec/changes/<name>/specs/` 下存在 delta specs：
     - 提取全部 requirement（匹配 "### Requirement:"）
     - 对每个 requirement：
       - 在代码库中搜索相关关键词
       - 评估实现是否可能存在
     - 若 requirement 看起来未实现：
       - 加入 CRITICAL："Requirement not found: <requirement name>"
       - 建议："Implement requirement X: <description>"

6. **验证 Correctness（正确性）**

   **Requirement 实现映射**：
   - 对每个 delta spec 的 requirement：
     - 在代码库中搜索实现证据
     - 若找到，记录文件路径和行范围
     - 评估实现是否符合 requirement 意图
     - 若发现偏离：
       - 加入 WARNING："Implementation may diverge from spec: <details>"
       - 建议："Review <file>:<lines> against requirement X"

   **Scenario 覆盖**：
   - 对每个 delta spec scenario（匹配 "#### Scenario:"）：
     - 检查代码是否处理了该条件
     - 检查是否存在覆盖该 scenario 的测试
     - 若看起来未覆盖：
       - 加入 WARNING："Scenario not covered: <scenario name>"
       - 建议："Add test or implementation for scenario: <description>"

7. **验证 Coherence（一致性）**

   **设计遵循**：
   - 若 `contextFiles` 中存在 design.md：
     - 提取关键决策（如 "Decision:"、"Approach:"、"Architecture:"）
     - 验证实现是否遵循这些决策
     - 若发现冲突：
       - 加入 WARNING："Design decision not followed: <decision>"
       - 建议："Update implementation or revise design.md to match reality"
   - 若没有 design.md：跳过该检查并注明 "No design.md to verify against"

   **代码模式一致性**：
   - 检查新增代码是否与项目现有模式一致
   - 检查文件命名、目录结构、编码风格
   - 若存在明显偏离：
     - 加入 SUGGESTION："Code pattern deviation: <details>"
     - 建议："Consider following project pattern: <example>"

8. **生成验证报告**

   **汇总评分卡**：
   ```
   ## Verification Report: <change-name>

   ### Summary
   | Dimension    | Status           |
   |--------------|------------------|
   | Completeness | X/Y tasks, N reqs|
   | Correctness  | M/N reqs covered |
   | Coherence    | Followed/Issues  |
   ```

   **按优先级列问题**：

   1. **CRITICAL**（归档前必须修复）：
      - 未完成任务
      - 缺失的 requirement 实现
      - 每项都要给出具体、可执行建议

   2. **WARNING**（建议修复）：
      - spec/design 与实现偏离
      - scenario 覆盖缺失
      - 每项都要给出具体建议

   3. **SUGGESTION**（可选优化）：
      - 模式一致性问题
      - 小幅改进项
      - 每项都要给出具体建议

   **最终结论**：
   - 若有 CRITICAL："X critical issue(s) found. Fix before archiving."
   - 若仅 WARNING："No critical issues. Y warning(s) to consider. Ready for archive (with noted improvements)."
   - 若全部通过："All checks passed. Ready for archive."

**验证启发式**

- **Completeness**：优先客观清单项（复选框、requirement 列表）
- **Correctness**：使用关键词搜索、路径分析与合理推断，不要求绝对确定性
- **Coherence**：关注明显不一致，不做风格吹毛求疵
- **误报控制**：不确定时，优先降级到 SUGGESTION，再到 WARNING，最后才是 CRITICAL
- **可执行性**：每个问题都必须给出可执行建议，并尽量附文件/行定位

**优雅降级**

- 仅存在 tasks.md：只验证任务完成度，跳过 spec/design
- 存在 tasks + specs：验证完整性与正确性，跳过 design
- 工件完整：执行三维度全检
- 始终注明跳过了哪些检查以及原因

**输出格式**

使用清晰的 markdown，包含：
- 汇总评分表
- 按 CRITICAL/WARNING/SUGGESTION 分组的问题列表
- 代码引用格式：`file.ts:123`
- 具体、可执行建议
- 避免含糊建议（如 "consider reviewing"）
