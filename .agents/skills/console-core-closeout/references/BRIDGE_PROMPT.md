# Console Core Closeout Bridge Prompt

请根据本窗口从开始到现在真实发生的 Console Core 工作，直接生成一份可粘贴到 Codex 的 `$console-core-closeout` 完整调用包。

本提示词只整理当前 Chat 工作现场。它不审计本地仓库、不修改文件、不运行代码或 Notebook、不操作 Git，也不修改 Shopify、Google Sheets、Google Drive、Workspace_Control、Personal_Knowledge_Base 或任何云端资源。正式 repository、branch、HEAD、worktree、Current、module version、Runtime provenance 和 evidence 必须由 Codex 在 Console Core 本地重新验证。

目标仓库：

```text
/Users/nikki/Documents/AI_Workspace/Projects/Console_Core
```

固定参数：

```text
PACKAGE_SCHEMA_VERSION: 1
TARGET_PROJECT_PATH: /Users/nikki/Documents/AI_Workspace/Projects/Console_Core
SOURCE_MODE: USER_PROVIDED_SOURCE_PACKAGE
ACCEPTANCE_SCOPE: TASK_ONLY
EXECUTION_MODE: AUDIT_THEN_CONFIRM
COMMIT_MODE: NO_COMMIT
```

自动判断并填写：

- `TASK_TYPE`
- `TASK_SCOPE`
- `EXPECTED_BRANCH`
- `EXPECTED_DIRTY_FILES`
- `ACCEPTANCE_TARGET`
- `SOURCE_PACKAGE`

`SOURCE_PACKAGE` 必须包含：

1. 项目和本轮任务的明确范围，以及明确不做事项；
2. 实际完成的代码、Runner、文档或证据工作；
3. 用户确认的 Python、Notebook Runner、文档、expected-version 和 Git Current；
4. 实际文件变化，以及 sibling workspace 或云端是否发生变化；
5. 已执行的验证和其结果；
6. evidence 属于 `EXACT_CURRENT`、`PREVIOUS_BYTES` 或 `EVIDENCE_GAP`；
7. evidence 属于 Preview、Dry Run、Live Run、business reconciliation 或 user acceptance；
8. planned operations 与 actual side effects，包括 known counters 和 partial outcomes；
9. `COMPLETED`、`CURRENT_STAGE`、唯一 `NEXT_ACTION` 和单独的长期 Pending；
10. 可能误导 Current 的旧资产、saved output、Runtime Cache 或版本不一致；
11. Console Core 项目专属 Pitfall 候选；
12. Workspace_Control、PKB 或 project structure upgrade 候选；
13. 未完成、未确认、失败、跳过及需要本地重新验证的事项。

只记录真实发生的工作，并明确区分：

```text
IMPLEMENTED
VALIDATED
COMMITTED
MERGED
PUSHED
PUBLISHED
CLOUD_SYNCED
NOT_COMPLETED
NEXT_ACTION
```

建议、计划、待办、生成的代码但未落地的内容、未运行的验证、未执行的 Runner alignment、未授权的 commit/push/publish，都不能写成完成。

事实可使用：

```text
USER_CONFIRMED
USER_PROVIDED
RUN_EVIDENCE
REPOSITORY_EVIDENCE
INFERENCE
NEEDS_CONFIRMATION
```

历史运行或旧版本 evidence 必须绑定旧 bytes；不得把它描述为 exact-current。Notebook 保存输出、Runtime Cache、聊天摘要和 Bridge Package 都不是 Current authority。

最终只输出一个纯文本代码块，不添加代码块外说明：

```text
$console-core-closeout

PACKAGE_SCHEMA_VERSION:
1

TARGET_PROJECT_PATH:
/Users/nikki/Documents/AI_Workspace/Projects/Console_Core

SOURCE_MODE:
USER_PROVIDED_SOURCE_PACKAGE

SOURCE_PACKAGE:
<完整内容；不得留下占位符，未知事实写 NEEDS_CONFIRMATION>

TASK_TYPE:
<自动选择>

TASK_SCOPE:
<一句具体范围，并明确排除长期待办>

ACCEPTANCE_SCOPE:
TASK_ONLY

EXPECTED_BRANCH:
<自动填写；未知写 NEEDS_CONFIRMATION>

EXPECTED_DIRTY_FILES:
<自动填写；未知写 NEEDS_CONFIRMATION>

ACCEPTANCE_TARGET:
<按本窗口最高且真实的证据选择>

EXECUTION_MODE:
AUDIT_THEN_CONFIRM

COMMIT_MODE:
NO_COMMIT
```

不要直接生成 Workspace_Control、PKB 或 structure-upgrade 的执行结果。正式 closeout 在完成本地审计后，若确有需要，必须输出对应的完整下游调用包。
