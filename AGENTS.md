# Console Core — Agent Rules

## Required Reading

For every task in this project, read:

1. `PROJECT.md`
2. This `AGENTS.md`

Then read only the task-relevant owner:

- package or module responsibility: `docs/CODE_MAP.md`
- stage, grain, key, or data movement: `docs/DATA_LINEAGE.md`
- Notebook, Runtime, progress, Preview, write gate, or result behavior: `docs/RUNTIME_CONTRACT.md`

Do not infer production readiness from filenames, directory presence, imports, or prior Notebook output.

## File Responsibilities

- `PROJECT.md` owns identity, boundary, Current, phase, maintenance map, and the unique next action.
- `README.md` is navigation and usage orientation, not project status authority.
- `docs/CODE_MAP.md` owns package/module status and overlap evidence.
- `docs/DATA_LINEAGE.md` owns pipeline stages, data ownership, grain, keys, and handoffs.
- `docs/RUNTIME_CONTRACT.md` owns the Colab/Python loading, progress, safety-gate, and result contract.
- Project-specific business rules belong in a dedicated owner only after a real continuing responsibility exists.

Do not duplicate detailed Current or module status across these files.

## Current and Version Rules

- Stable Current files are updated in place. Do not create `final`, `new`, `fixed`, dated, numbered, or otherwise parallel Current copies.
- The `Console_Core` Git `main` branch is authoritative for Python, requirements, future tests, and formal project documentation.
- `Console_Core_Colab` is a Notebook Runner / operational workspace, not Python source or Git-history authority.
- `.runtime/ecom` is a disposable Runtime Cache, not an independent project or second source repository.
- A loaded Colab module is not assumed current until repository, branch, commit SHA, module path, and module version are shown.

## Repository and Change Safety

- Default to one primary Git repository per task.
- Preserve unrelated user changes. Never stash, reset, overwrite, or clean them without explicit authorization.
- Check Git root, branch, HEAD, upstream divergence, staged, unstaged, and untracked files before changes.
- Do not modify the sibling Colab workspace merely because the main repository is in scope.
- Do not delete, move, rename, merge, or retire historical candidates based only on similarity or filename.
- Do not change the `nikkilog/ecom.git` remote or repository identity without separate approval.
- Do not commit, push, publish, release, or cloud-sync without explicit authorization for that action.

## Input and Output Boundaries

Before implementation, identify:

- business objective;
- input and output;
- entity/grain;
- stable unique key;
- source of truth;
- acceptance conditions;
- external side effects.

Input rows are not interchangeable with business objects or API operations. Preview/planned counts must never be reported as actual inserted or written counts.

## Shopify Write Safety

Default all real-write workflows to:

```text
DRY_RUN = True
CONFIRMED = False
```

External modification is allowed only when:

```text
DRY_RUN = False
CONFIRMED = True
```

High-risk workflows may add `ALLOW_REAL_WRITE`, `REQUIRE_APPROVED`, maximum-object limits, and an expected module-version gate.

- Read all inputs and validate the complete plan before executing writes.
- Use immutable IDs before mutable labels such as SKU or handle when locating owners.
- Re-read current Shopify facts before reference differences, handle-sensitive creation, or other state-dependent writes.
- Define `CLEAR` per module and field type; it is not one universal technical operation.
- Preserve list and structured-field order.
- Keep partially created products in `DRAFT` and report partial failure.
- Do not let validation, Preview, or `DRY_RUN` produce undisclosed side effects.

## Secret and Publishing Rules

- Never place Secret values, passwords, tokens, private keys, authentication JSON, or OAuth credentials in Git, Markdown, Notebook output, or chat.
- Configuration may store non-sensitive Secret names, domains, API versions, public identifiers, and routing information.
- Do not display, move, rename, copy, commit, upload, or create Secret material without explicit authorization.
- Google Drive, Sheets, Colab, and Shopify are operational systems, not automatic Current authorities.
- No push, publishing, release, Google Drive synchronization, Sheet mutation, or Shopify write is implicit in a code or documentation task.

## Runtime and Progress Rules

Follow `docs/RUNTIME_CONTRACT.md`.

Long-running work must emit readable progress: phase, completed/total, current object or batch, succeeded, failed, skipped, warnings, errors, retries/throttling, fallback state, and periodic heartbeat or ETA where practical. Avoid both silent execution and uncontrolled per-row output.

## Current Phase Gates

The project is in `ONBOARDING`.

- Existing modules are `ACTIVE_BUT_UNVALIDATED`, `EXPERIMENTAL`, `HISTORICAL_CANDIDATE`, `EMPTY_PLACEHOLDER`, or `BOUNDARY_CONFLICT` unless documented evidence supports a stronger status.
- Do not describe modules as production-ready without end-to-end validation.
- First resolve authority, call sites, contracts, and test coverage before deleting historical candidates or broadening write operations.
- Configuration synchronizers require the same Preview, diff, confirmation, affected-row, and RunLog discipline as other writers.

## Closeout Method

Use `$console-core-closeout` for Console Core project closeout.

Closeout must report validation, warnings, unresolved evidence, Git status, commit/push/cloud-sync state, and the safe resume point. Identity or lifecycle changes require a separate Workspace Control synchronization task.
