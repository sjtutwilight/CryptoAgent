---
name: openspec-onboard
description: OpenSpec 引导式入门——通过讲解与真实代码库操作带用户走完完整工作流。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

引导用户完成他们的第一次 OpenSpec 全流程。 这是一个教学体验：你要在他们的真实代码库里做真实工作，并解释每一步。

---

## 预检（Preflight）

开始前，检查 OpenSpec 是否已初始化：

```bash
openspec status --json 2>&1 || echo "NOT_INITIALIZED"
```

**如果尚未初始化：**
> OpenSpec 还没有在这个项目中完成初始化。请先运行 `openspec init`，然后再回到 `/opsx:onboard`。

如果未初始化，就在这里停止。

---

## 阶段 1：欢迎（Welcome）

展示：

```
## Welcome to OpenSpec!

我会带你走一遍完整的 change 生命周期——从想法到实现——并且使用你代码库里的真实任务。你会在“做中学”里掌握这个工作流。

**我们会做什么：**
1. 在你的代码库里选一个小而真实的任务
2. 简短探索问题
3. 创建一个 change（承载本次工作的容器）
4. 构建工件：proposal → specs → design → tasks
5. 实现 tasks
6. 归档已完成 change

**时间：** 约 15-20 分钟

先从找一个要做的任务开始。
```

---

## 阶段 2：任务选择（Task Selection）

### 代码库分析

扫描代码库，找小型改进机会。优先看：

1. **TODO/FIXME 注释** - 在代码文件中搜索 `TODO`、`FIXME`、`HACK`、`XXX`
2. **错误处理缺失** - 吞掉异常的 `catch`、高风险操作无 try-catch
3. **无测试函数** - 交叉对照 `src/` 与测试目录
4. **类型问题** - TypeScript 文件中的 `any`（`: any`、`as any`）
5. **调试残留** - 非调试代码中的 `console.log`、`console.debug`、`debugger`
6. **校验缺失** - 用户输入处理处缺少校验

同时查看最近 git 活动：
```bash
git log --oneline -10 2>/dev/null || echo "No git history"
```

### 给出建议

基于扫描结果，给出 3-4 个具体建议：

```
## Task Suggestions

基于代码库扫描，我建议以下入门任务：

**1. [最优先建议]**
   位置：`src/path/to/file.ts:42`
   范围：约 1-2 个文件，约 20-30 行
   适合原因：[简述]

**2. [第二建议]**
   位置：`src/another/file.ts`
   范围：约 1 个文件，约 15 行
   适合原因：[简述]

**3. [第三建议]**
   位置：[location]
   范围：[estimate]
   适合原因：[简述]

**4. 其他想法？**
   也可以直接告诉我你想做什么。

你想选哪个任务？（回复编号或描述你自己的任务）
```

**如果没有明显机会：**退回询问用户想做什么：
> 我暂时没在代码库里发现特别明显的 quick win。你有没有一个一直想补的小功能或小修复？

### 范围护栏（Scope Guardrail）

如果用户选择的任务过大（大型功能、多日工作）：

```
这个任务很有价值，但作为第一次 OpenSpec 演练，范围可能偏大。

为了学习流程，先做小一些更好——这样你能完整体验一遍，而不会卡在实现细节里。

**可选方式：**
1. **切小范围** - [你的任务] 最小可交付切片是什么？比如只做 [具体切片]？
2. **换一个任务** - 选前面建议里的其他小任务，或给一个新的小任务。
3. **坚持原任务** - 也可以继续做，只是整体会更久。

你希望怎么选？
```

如果用户坚持，允许覆盖这个软护栏。

---

## 阶段 3：探索模式演示（Explore Demo）

任务选定后，简短演示 explore 模式：

```
在创建 change 前，我先快速演示一下 **explore mode**——它用于在确定方向前进行问题思考。
```

花 1-2 分钟调查相关代码：
- 读取相关文件
- 如有帮助，画一个简短 ASCII 图
- 记录关键考量点

```
## Quick Exploration

[你的简短分析：发现了什么、要注意什么]

┌─────────────────────────────────────────┐
│   [可选：如果有帮助，放一张 ASCII 图]    │
└─────────────────────────────────────────┘

Explore mode（`/opsx:explore`）就是做这类思考：先调查，再实现。你在任何需要理清问题的时候都可以使用它。

现在我们创建一个 change 来承载这次工作。
```

**暂停（PAUSE）** - 等用户确认后再继续。

---

## 阶段 4：创建 Change（Create the Change）

**先解释（EXPLAIN）：**
```
## Creating a Change

在 OpenSpec 里，"change" 是一份工作的思考与计划容器。它位于 `openspec/changes/<name>/`，里面包含 proposal、specs、design、tasks 等工件。

我先为这个任务创建一个 change。
```

**执行（DO）：** 用推导出的 kebab-case 名称创建 change：
```bash
openspec new change "<derived-name>"
```

**展示（SHOW）：**
```
已创建：`openspec/changes/<name>/`

目录结构：
```
openspec/changes/<name>/
├── proposal.md    ← 为什么做（当前为空，待填写）
├── design.md      ← 怎么做（当前为空）
├── specs/         ← 详细需求（当前为空）
└── tasks.md       ← 实现清单（当前为空）
```

接下来先填写第一个工件：proposal。
```

---

## 阶段 5：Proposal

**先解释（EXPLAIN）：**
```
## The Proposal

proposal 记录的是我们为什么做这件事，以及高层会改什么。它是这项工作的“电梯陈述”。

我先基于当前任务起草一版。
```

**执行（DO）：** 先草拟 proposal 内容（先不保存）：

```
这是提案草稿：

---

## Why

[1-2 句，说明问题/机会]

## What Changes

[列出将发生的变化]

## Capabilities

### New Capabilities
- `<capability-name>`: [简述]

### Modified Capabilities
<!-- 若修改现有行为则填写 -->

## Impact

- `src/path/to/file.ts`: [变更说明]
- [其他文件（如适用）]

---

这版是否符合你的意图？可以先改再保存。
```

**暂停（PAUSE）** - 等用户确认/反馈。

确认后保存 proposal：
```bash
openspec instructions proposal --change "<name>" --json
```
然后将内容写入 `openspec/changes/<name>/proposal.md`。

```
Proposal 已保存。这份文档记录的是“为什么做”，后续理解变化时也可以回头修订。

下一步：specs。
```

---

## 阶段 6：Specs

**先解释（EXPLAIN）：**
```
## Specs

specs 以精确、可验证的方式定义“做什么”。它使用 requirement/scenario 格式，让预期行为清晰可测。

像当前这种小任务，通常一个 spec 文件就够了。
```

**执行（DO）：** 创建 spec 文件目录：
```bash
mkdir -p openspec/changes/<name>/specs/<capability-name>
```

起草 spec 内容：

```
这是 spec：

---

## ADDED Requirements

### Requirement: <Name>

<系统需要做什么的描述>

#### Scenario: <Scenario name>

- **WHEN** <触发条件>
- **THEN** <预期结果>
- **AND** <可选补充结果>

---

这种 WHEN/THEN/AND 格式让需求天然可测，几乎可以直接映射成测试用例。
```

保存到 `openspec/changes/<name>/specs/<capability>/spec.md`。

---

## 阶段 7：Design

**先解释（EXPLAIN）：**
```
## Design

design 记录“怎么做”——技术决策、权衡与实现路径。

小型变更的 design 可以很短，这完全正常，不需要过度设计。
```

**执行（DO）：** 起草 design.md：

```
这是 design：

---

## Context

[当前状态的简要背景]

## Goals / Non-Goals

**Goals:**
- [我们要达成什么]

**Non-Goals:**
- [明确不做什么]

## Decisions

### Decision 1: [关键决策]

[方案与理由说明]

---

对于小任务，这样的设计深度就足够承载关键决策。
```

保存到 `openspec/changes/<name>/design.md`。

---

## 阶段 8：Tasks

**先解释（EXPLAIN）：**
```
## Tasks

最后把工作拆成可实现任务——也就是 apply 阶段要逐项勾选的复选框。

任务应该小而清晰，并且顺序合理。
```

**执行（DO）：** 基于 specs 与 design 生成任务：

```
这是实现任务：

---

## 1. [类别或文件]

- [ ] 1.1 [具体任务]
- [ ] 1.2 [具体任务]

## 2. Verify

- [ ] 2.1 [验证步骤]

---

每个复选框都会变成 apply 阶段的一个工作单元。准备实现了吗？
```

**暂停（PAUSE）** - 等用户确认进入实现。

保存到 `openspec/changes/<name>/tasks.md`。

---

## 阶段 9：Apply（实现）

**先解释（EXPLAIN）：**
```
## Implementation

现在开始逐项实现任务，并在完成后勾选。我会逐项播报，必要时说明 specs/design 是如何影响实现决策的。
```

**执行（DO）：** 对每个任务：

1. 播报："Working on task N: [description]"
2. 在代码库实现变更
3. 自然引用 specs/design："spec 里要求 X，所以实现方式采用 Y"
4. 在 tasks.md 勾选完成：`- [ ]` → `- [x]`
5. 简短状态："✓ Task N complete"

讲解保持轻量，不要逐行教学。

全部任务完成后：

```
## Implementation Complete

All tasks done:
- [x] Task 1
- [x] Task 2
- [x] ...

实现已完成！还差最后一步：归档。
```

---

## 阶段 10：Archive

**先解释（EXPLAIN）：**
```
## Archiving

当 change 完成后，要进行归档。归档会把它从 `openspec/changes/` 移动到 `openspec/changes/archive/YYYY-MM-DD-<name>/`。

归档后的 change 会成为项目决策历史，后续可以随时回溯“为什么当时这么做”。
```

**执行（DO）：**
```bash
openspec archive "<name>"
```

**展示（SHOW）：**
```
已归档到：`openspec/changes/archive/YYYY-MM-DD-<name>/`

这个 change 已成为项目历史的一部分：代码在代码库里，决策记录在归档中。
```

---

## 阶段 11：复盘与下一步（Recap & Next Steps）

```
## Congratulations!

你刚完成了一次完整 OpenSpec 循环：

1. **Explore** - 思考并澄清问题
2. **New** - 创建 change 容器
3. **Proposal** - 记录 WHY
4. **Specs** - 定义 WHAT
5. **Design** - 决定 HOW
6. **Tasks** - 拆解执行步骤
7. **Apply** - 实现工作
8. **Archive** - 保存决策记录

这个节奏适用于任何规模变更：从小修复到大型功能。

---

## Command Reference

| Command | What it does |
|---------|--------------|
| `/opsx:explore` | 在工作前/工作中进行问题思考 |
| `/opsx:new` | 新建 change，并逐步创建工件 |
| `/opsx:ff` | 快进：一次创建全部工件 |
| `/opsx:continue` | 继续推进已有 change |
| `/opsx:apply` | 根据 change 实现 tasks |
| `/opsx:verify` | 验证实现是否匹配工件 |
| `/opsx:archive` | 归档已完成 change |

---

## What's Next?

在你真正想做的任务上试试 `/opsx:new` 或 `/opsx:ff`。你已经掌握这个节奏了！
```

---

## 优雅退出处理（Graceful Exit Handling）

### 用户希望中途停止

如果用户表示要暂停、要离开，或明显失去投入：

```
没问题！你的 change 已保存在 `openspec/changes/<name>/`。

之后继续可以用：
- `/opsx:continue <name>` - 继续创建工件
- `/opsx:apply <name>` - 若 tasks 已存在，可直接进入实现

工作不会丢失。你准备好时随时回来继续。
```

应优雅结束，不施压。

### 用户只想要命令速查

如果用户明确表示只看命令、跳过教程：

```
## OpenSpec Quick Reference

| Command | What it does |
|---------|--------------|
| `/opsx:explore` | 思考问题（不改业务代码） |
| `/opsx:new <name>` | 新建 change，分步推进 |
| `/opsx:ff <name>` | 快进：一次生成全部工件 |
| `/opsx:continue <name>` | 继续已有 change |
| `/opsx:apply <name>` | 实现任务 |
| `/opsx:verify <name>` | 验证实现 |
| `/opsx:archive <name>` | 完成后归档 |

你可以先试 `/opsx:new` 开始第一个 change；若想更快，直接 `/opsx:ff`。
```

优雅结束。

---

## 护栏（Guardrails）

- 在关键切换点遵循 **EXPLAIN → DO → SHOW → PAUSE** 模式（探索后、proposal 草稿后、tasks 后、archive 后）
- 实现阶段讲解保持轻量，教学但不过度说教
- 即使变更很小也不要跳阶段：目标是让用户看见完整流程
- 在标记点暂停等待确认，但不要频繁过度打断
- 用户想退出时要优雅处理，不要催促继续
- 使用真实代码库任务，不要模拟或虚构示例
- 温和引导任务范围缩小，但尊重用户最终选择
