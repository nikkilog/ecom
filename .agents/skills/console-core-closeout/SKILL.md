---
name: console-core-closeout
description: Audit and close out Console Core feature, bugfix, Python module, Notebook Runner, runtime-contract, documentation, evidence, release, or project-stage work. Preserve the unique Git and Runner Current, verify Python/Runner/version/provenance alignment, distinguish Preview and planned operations from actual side effects, route knowledge to its authoritative owner, and close Git safely. Do not use for implementation, runtime execution, external writes, or direct changes to Console_Core_Colab, Workspace_Control, PKB, or cloud resources.
---

# Console Core Project Closeout

## Purpose

Leave Console Core understandable and safely resumable after a bounded task or stage. Preserve its authority boundaries, exact Current, evidence limits, progress, single next action, and Git state without turning closeout into development, runtime execution, shipping, or a cross-repository mutation workflow.

Before adding a step, ask whether it changes a decision, produces a required output, or reduces a concrete risk. Omit it otherwise.

## Required Inputs

Collect or infer these fields and mark uncertainty explicitly:

```text
PACKAGE_SCHEMA_VERSION: 1
TARGET_PROJECT_PATH
SOURCE_MODE: CURRENT_CODEX_THREAD | USER_PROVIDED_SOURCE_PACKAGE | LOCAL_SOURCE_FILE | REPOSITORY_ONLY
SOURCE_PACKAGE
SOURCE_FILE
TASK_TYPE: FEATURE | BUGFIX | CODE_MIGRATION | NOTEBOOK_RUNNER | DOCUMENT_UPDATE | EVIDENCE_CLOSEOUT | RELEASE_CLOSEOUT | PROJECT_CLOSEOUT
TASK_SCOPE
ACCEPTANCE_SCOPE: TASK_ONLY | PROJECT_HEALTH | RELEASE
EXPECTED_BRANCH
EXPECTED_DIRTY_FILES
ACCEPTANCE_TARGET: DESIGNED | IMPLEMENTED | STATICALLY_VERIFIED | TEST_VERIFIED | DRY_RUN_VERIFIED | LIVE_RUN_VERIFIED | BUSINESS_RECONCILED | ACCEPTED | FROZEN
EXECUTION_MODE: AUDIT_ONLY | AUDIT_THEN_CONFIRM | DIRECT_EXECUTE_AFTER_SAFE_GATE
COMMIT_MODE: NO_COMMIT | COMMIT_AFTER_VALIDATION
```

Defaults:

```text
PACKAGE_SCHEMA_VERSION: 1
TARGET_PROJECT_PATH: /Users/nikki/Documents/AI_Workspace/Projects/Console_Core
SOURCE_MODE: CURRENT_CODEX_THREAD
ACCEPTANCE_SCOPE: TASK_ONLY
EXECUTION_MODE: AUDIT_THEN_CONFIRM
COMMIT_MODE: NO_COMMIT
```

For `USER_PROVIDED_SOURCE_PACKAGE`, require `SOURCE_PACKAGE`. For `LOCAL_SOURCE_FILE`, require a readable `SOURCE_FILE`. Treat either as leads, not repository or evidence authority. Under `REPOSITORY_ONLY`, use repository facts and recorded carry-forward state without inventing chat history.

## Scope

Close out only the authorized Console Core work. Do not:

- implement business behavior or broaden the task;
- execute Notebook Runners or Shopify workflows merely to improve an evidence label;
- modify `Console_Core_Colab` because Python changed;
- treat `.runtime/ecom` as source or Current;
- commit, merge, push, publish, cloud-sync, or perform external writes without explicit authorization;
- modify Workspace_Control, Personal_Knowledge_Base, another first-level project, Shopify, Google Sheets, or Google Drive;
- expose, copy, move, or create Secret, OAuth, token, or credential material.

## Minimum Reading Order

Read only what is needed for the closeout questions:

1. target `PROJECT.md`;
2. target `AGENTS.md`;
3. task-relevant code, Notebook facts, tests, run evidence, diff, and history;
4. `docs/CODE_MAP.md` for package/module responsibility, status, overlap, or Runner relationships;
5. `docs/DATA_LINEAGE.md` for stage, grain, stable key, ownership, or handoff changes;
6. `docs/RUNTIME_CONTRACT.md` for Runner, provenance, version gate, Preview, writes, progress, Result, or RunLog;
7. `README.md` only when navigation or usage orientation changed.

Load the references directly from this Skill as decisions require:

- ordinary Chat handoff package generation: [BRIDGE_PROMPT.md](references/BRIDGE_PROMPT.md)
- authority, repository boundaries, or document owners: [authority-and-scope.md](references/authority-and-scope.md)
- Python/Runner alignment, provenance, writes, counters, Result, or RunLog: [runtime-and-runner-closeout.md](references/runtime-and-runner-closeout.md)
- Current identity, asset hygiene, evidence, acceptance, or findings: [acceptance-and-current.md](references/acceptance-and-current.md)
- project knowledge, Pitfalls, Workspace_Control, PKB, or structure-upgrade routing: [knowledge-routing.md](references/knowledge-routing.md)

Do not perform a full-repository audit by default. Expand only when sources conflict or task-relevant ownership is unclear.

## Closeout Questions

Make the repository answer:

- What does the affected responsibility do, and which owner explains it?
- Which Python, Runner, documentation, expected-version contract, branch, and commit are task-relevant Current?
- Are Python Git Current and Runner Current aligned, independently stale, or pending follow-up?
- What actually completed, what was validated, and which bytes does the evidence cover?
- Is the evidence Preview, Dry Run, Live Run, reconciliation, or acceptance?
- Which counts are planned and which are actual side effects?
- What is `COMPLETED`, what is `CURRENT_STAGE`, and what is the single `NEXT_ACTION`?
- Could an old, duplicate, cached, or saved-output asset mislead future work?
- What Git or downstream action is authorized now?

## Console Core Gates

### Authority and Current

Apply [authority-and-scope.md](references/authority-and-scope.md). Report task-relevant dimensions separately:

```text
PYTHON_CURRENT
RUNNER_CURRENT
DOCUMENTATION_CURRENT
EXPECTED_VERSION_ALIGNMENT
GIT_CURRENT
```

One dimension must not overwrite another. Git Python may be Current while a Runner remains stale or pending alignment.

### Runner and Runtime

When a Python module, Runner, Runtime, dependency, or expected-version contract is involved, apply [runtime-and-runner-closeout.md](references/runtime-and-runner-closeout.md). Determine whether a relevant Runner exists and whether module reference, dependency contract, expected version, provenance, stale-module protection, and clean-kernel acceptance remain aligned.

Do not update the sibling Runner workspace automatically. Record a required Runner change as `CURRENT_STAGE`, the single `NEXT_ACTION`, or an independent bounded follow-up scope.

### Writes and Execution Evidence

Classify evidence as Preview, Dry Run, or Live. Preserve the real write gates and distinguish planned work from attempted, succeeded, failed, skipped, no-change, and written outcomes. Result and RunLog support execution claims; they never become Current or project-state authority.

### Documents and Knowledge

Apply [knowledge-routing.md](references/knowledge-routing.md). Update only a document whose owned relationship changed. Do not update every owner for an ordinary bugfix or create a new broad summary.

### Acceptance

Apply [acceptance-and-current.md](references/acceptance-and-current.md). Keep `CURRENT_IDENTITY`, `TASK_ACCEPTANCE`, `PROJECT_HEALTH`, and `RELEASE_READINESS` independent. Bind evidence to `EXACT_CURRENT`, `PREVIOUS_BYTES`, or `EVIDENCE_GAP` and use only the strongest proven acceptance level.

## Repository Gate

Before any write, verify:

```text
repository_root
branch
HEAD
remote and upstream divergence
staged files
unstaged files
untracked files
expected dirty files
unrelated user changes
Current identity conflicts
commit authorization
push authorization
publish/cloud-sync authorization
```

Stop writes on an unexpected repository, unexplained changes, conflicting Current identity, required out-of-scope repository modification, or missing evidence that would force invention. Never stash, reset, restore, clean, or overwrite user work.

## Two-Phase Execution

For `AUDIT_THEN_CONFIRM`:

1. perform a read-only audit;
2. return one exact bounded execution package with files, intended changes, validations, and exclusions;
3. wait for explicit confirmation;
4. modify only the confirmed package;
5. validate, inspect the diff, and report actual results.

For `AUDIT_ONLY`, never modify files. For `DIRECT_EXECUTE_AFTER_SAFE_GATE`, execute only a previously explicit and safe package. Commit only when `COMMIT_AFTER_VALIDATION` and separate commit authorization are both present. Never infer merge, push, publish, or cloud-sync authorization.

## Progress and Next Action

Report separately:

```text
COMPLETED: facts that actually happened
CURRENT_STAGE: where the scoped work now stands
NEXT_ACTION: exactly one concrete next step
LONG_TERM_PENDING: broader items that do not replace NEXT_ACTION
```

Do not describe a suggestion, planned Runner alignment, unperformed acceptance, commit, push, publication, or cloud synchronization as complete.

## Downstream Routing

This Skill never edits another repository. Always report:

```text
PROJECT_STRUCTURE_UPGRADE_RECOMMENDED: YES | NO
PROJECT_UPGRADE_CALL_PACKAGE: <complete package when YES; N/A when NO>
WORKSPACE_CONTROL_SYNC_REQUIRED: YES | NO
WORKSPACE_SYNC_CALL_PACKAGE: <complete package when YES; N/A when NO>
PKB_SYNC_REQUIRED: YES | NO
PKB_ABSORPTION_CALL_PACKAGE: <complete package when YES; N/A when NO>
```

Every `YES` requires a complete, directly usable downstream package. Follow [knowledge-routing.md](references/knowledge-routing.md). Do not call this Skill from its own output.

## Report Contract

### Phase One

Report, in order:

```text
SOURCE_MODE
TASK_SCOPE
REPOSITORY_CONTEXT
SOURCES_READ
CURRENT_IDENTITY
COMPLETED
CURRENT_STAGE
NEXT_ACTION
DOCUMENTATION_AND_KNOWLEDGE_PLAN
RUNNER_RUNTIME_REVIEW
WRITE_AND_EVIDENCE_BOUNDARY
CURRENT_ASSET_REVIEW
GIT_REVIEW
DOWNSTREAM_ROUTING
FILES_PROPOSED
FILES_EXCLUDED
RISKS_OR_EVIDENCE_GAPS
BOUNDED_EXECUTION_PACKAGE
PHASE_ONE_RESULT: READY_FOR_CONFIRMATION | BLOCKED
MUTATION_PERFORMED: NO
```

### Final

Report actual files and validation, the five Current dimensions, four acceptance dimensions, completed work, current stage, one next action, evidence boundary, Git state, authorization outcomes, downstream routing, safe resume point, and exactly one disposition:

```text
ACTION_COMPLETED_NOW
ALREADY_IN_DESIRED_STATE
BLOCKED
```

Do not let broader project-health gaps override a completed task unless they violate the task scope or acceptance target.
