# Runtime and Runner Closeout

## Python and Runner Alignment

When task-relevant Python changes, determine:

1. whether a related Notebook Runner exists;
2. whether it imports or invokes the changed module or function;
3. whether its expected module version or revision gate matches Git Current;
4. whether dependency versions and invocation parameters remain compatible;
5. whether the Runner prints correct provenance and prevents stale imports;
6. whether acceptance requires a clean Kernel.

Report Python and Runner Current independently. Do not modify `Console_Core_Colab` automatically. If alignment remains, record an independent bounded follow-up as `CURRENT_STAGE` and `NEXT_ACTION` without describing it as complete.

Do not hardcode temporary module versions in this Skill. Read versions and gates from task-relevant Current artifacts.

## Provenance and Stale Modules

When Runtime, Runner, module identity, or version gates matter, require safe evidence for:

```text
repository
branch
commit_sha
module_path
module_version
loaded_function_source
```

`loaded_function_source` may be an actual safe source location or fingerprint. It must not expose credentials.

Updating Git does not prove a running Kernel loaded new code. Saved Notebook output, globals, `sys.modules`, local `__pycache__`, and `.runtime/ecom` copies are runtime history or cache, not Current acceptance.

The Runtime must fail closed when a required revision or expected-version gate does not match. High-risk Apply chains may enforce the version relationship immediately after load and again before Apply.

## Clean-Kernel Boundary

Require clean-Kernel acceptance when a module or expected-version gate changed and the acceptance target depends on Runtime behavior. Start from the first cell and capture provenance before execution.

Do not rerun solely to raise an evidence label. Require the run only when the missing proof changes correctness, safety, Current identity, a decision, or a required output.

## Preview, Dry Run, and Live

Classify execution evidence explicitly:

- `PREVIEW`: a complete validated plan; no proof of external modification.
- `DRY_RUN`: execution with business writes disabled; disclose every remaining side effect.
- `LIVE`: confirmed external effects with actual outcome and reconciliation evidence.

Console Core real-write defaults are:

```text
DRY_RUN = True
CONFIRMED = False
```

Real external modification normally requires:

```text
DRY_RUN = False
CONFIRMED = True
```

High-risk flows may additionally require `ALLOW_REAL_WRITE`, `REQUIRE_APPROVED`, bounded maximum-object gates, and expected module versions. Closeout verifies what the Current contract requires and what the evidence proves; it does not execute writes.

Validation, Preview, and Dry Run must not produce undisclosed Sheet creation, mapping changes, RunLog writes, cache mutations, or other side effects.

## Planned and Actual Counters

Keep these meanings separate:

```text
rows_loaded
rows_validated
business_objects_planned
api_operations_planned
api_operations_attempted
api_operations_succeeded
api_operations_failed
rows_written
skipped_count
warning_count
error_count
```

Input rows are not business objects or API operations. Planned `inserted`, `created`, or `written` values must be labeled planned. Only confirmed effects may be reported as actual writes.

Keep `NO_CHANGE`, explicit skips, validation errors, warnings, execution failures, and partial successes distinct. A no-change result is not automatically a skipped row or a write.

## Result and RunLog

Result and RunLog provide execution evidence. They must:

- separate Preview and execution phases;
- preserve planned versus actual counts;
- record succeeded and failed operations for partial success;
- distinguish no-change, skipped, validation failure, and execution failure;
- identify operation and stable business-object or trace identity;
- disclose retries, throttling, fallback, and remaining side effects;
- avoid Secret values in results, logs, metadata, or exceptions.

They do not own project Current, Python Current, Runner Current, or project status.

Shopify effects and Google Sheets Result/RunLog writes are not one transaction. Evidence that one succeeded cannot silently prove the other succeeded. Closeout must preserve recovery requirements and must not describe partial work as complete.
