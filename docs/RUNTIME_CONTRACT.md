# Console Core Runtime Contract

## Authority Relationship

```text
Console_Core Git main
= authoritative Python modules, requirements, future tests, and formal docs

Console_Core_Colab
= thin Notebook Runners and operational launch surfaces
!= authoritative Python source
!= Git history

.runtime/ecom
= disposable clone/cache loaded from Git
!= second source repository
!= independent project
```

Notebook code should progressively shrink to configuration, loading, invocation, and presentation. Core business logic belongs in Git-hosted Python modules.

## Logical Notebook Structure

Historical Runners may physically use more cells, but they should map to three logical sections.

### Config

Contains explicit runtime parameters:

- `site_code`;
- Console route or URL;
- non-sensitive Secret names;
- Sheet label and Worksheet title;
- `DRY_RUN`;
- `CONFIRMED`;
- bounded-run switches such as maximum products or variants;
- expected module version or commit where risk warrants it.

Every visible parameter must either be passed to the invoked Python function or be marked display-only/deprecated. A parameter that exists in a cell but is not wired into execution is a contract failure.

Defaults for real-write workflows:

```text
DRY_RUN = True
CONFIRMED = False
```

### Load

The Load section:

1. authenticates through approved Runtime Secret facilities without printing values;
2. installs or validates dependencies;
3. cleanly clones or fetches the configured repository;
4. checks out the intended branch and revision;
5. prevents stale-module reuse;
6. imports the exact Python module;
7. prints provenance before Run.

Required provenance output:

```text
repository
branch
commit_sha
module_path
module_version
loaded_function_source
```

`loaded_function_source` should identify the actual function file/source location or a safe source fingerprint. It must not expose credentials.

### Run

The Run section:

- invokes Python with the displayed Config parameters;
- displays validation and Preview before any real write;
- shows bounded, readable progress;
- returns structured status, summary, warnings, errors, and metadata;
- distinguishes planned operations from actual side effects.

## Preventing Stale Colab Modules

Updating GitHub does not prove that a running Kernel loaded new code.

Use one or more reliable mechanisms:

- clean disposable clone;
- `fetch` plus explicit checkout and fast-forward update;
- clear relevant entries from `sys.modules`;
- `importlib.invalidate_caches()` and `importlib.reload()`;
- load by an exact file path;
- enforce a minimum or expected module version;
- print `module.__file__`;
- print the repository commit SHA used by the module.

The Runtime must fail closed when the expected module version or revision gate does not match.

## Progress and Heartbeat

Long tasks must not remain silent.

Progress output should include, where applicable:

- current phase;
- completed and total objects;
- current object or batch;
- succeeded;
- failed;
- skipped;
- `warning_count`;
- `error_count`;
- current retry or throttling state;
- fallback or degraded mode;
- phase start and finish;
- ETA or periodic heartbeat when practical.

Preferred output patterns:

- one update per batch or every N objects;
- single-line dynamic refresh for high-frequency counters;
- phase summaries;
- periodic heartbeat;
- errors grouped by type with limited examples.

Avoid unbounded row-by-row output. An operator must be able to tell whether the task is alive, where it is, whether it is blocked or throttled, and roughly how much work remains.

## Preview and Side-Effect Contract

Preview describes a complete validated plan. It is not evidence of external modification.

Every workflow must distinguish:

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

Planned `inserted`, `written`, or `created` counts must be labeled as planned. Actual counts are emitted only after confirmed side effects.

`DRY_RUN` must disclose any remaining side effects, including Sheet creation, mapping writes, RunLog writes, cache changes, or other operational mutations. A dry run with undisclosed side effects violates this contract.

## Write Gates

Real external modification is permitted only when:

```text
DRY_RUN = False
CONFIRMED = True
```

High-risk workflows may also require:

```text
ALLOW_REAL_WRITE = True
REQUIRE_APPROVED = True
MAX_PRODUCTS = <bounded integer>
MAX_VARIANTS = <bounded integer>
EXPECTED_MODULE_VERSION = <approved version>
```

Failure of any required gate returns a Preview, `SKIPPED`, or validation failure without real business-object writes.

## Unified Result Contract

Recommended top-level result:

```text
status
run_id
phase
summary
preview
warnings
errors
meta
```

Recommended statuses:

- `PREVIEW`
- `SUCCESS`
- `SUCCESS_WITH_WARNINGS`
- `PARTIAL_SUCCESS`
- `FAILED_VALIDATION`
- `FAILED_EXECUTION`
- `SKIPPED`
- `NO_CHANGE`

`summary` should contain the counters defined in the Preview section. `warnings` and `errors` are structured, counted, graded where applicable, aggregated by type, and include only limited safe examples. `meta` should contain provenance, timing, site/job identity, retry/fallback information, and safe trace data.

Secret values, authentication JSON, tokens, and private credentials never appear in results, progress, exceptions, or metadata.

## RunLog Contract

RunLog records actual phase and effect:

- Preview and execution have distinct phases.
- Planned counts do not become actual-write counts.
- Partial success records both succeeded and failed operations.
- No-change and skipped work are explicit.
- API operation type and business-object identity are traceable.
- Append operations verify the target header/schema contract first.
- Repeated runs use stable idempotency keys where the workflow requires append history or recovery.

## Failure and Recovery

- Validation failure occurs before writes whenever complete-plan validation is possible.
- Batch failures should be isolated to the responsible owner and field, including recursive splitting where appropriate.
- Retries and rate-limit waits are visible in progress and final metadata.
- Fallback/degraded behavior is explicit and never silently changes data meaning.
- Product-creation partial failure leaves the product DRAFT and records recovery information.
- The final result identifies safe resume conditions and does not describe partial work as complete.
