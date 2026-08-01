# Console Core Runtime Contract

## Authority Relationship

```text
Console_Core Git main
= authoritative Python modules, requirements, future tests, and formal docs

Console_Core_Colab
= thin Notebook Runners and operational launch surfaces
!= Git repository
!= authoritative Python source
!= Git history

.runtime/ecom
= disposable clone/cache loaded from Git
!= second source repository
!= independent project
```

Notebook code should progressively shrink to configuration, loading, invocation, and presentation. Core business logic belongs in Git-hosted Python modules.

The Console Core Git repository is Current for Python, tests, and formal
documentation. `Console_Core_Colab` carries Notebook entry points and Runtime
copies only. Notebook expected-version gates must align with Console Core
Current. Notebook Current must have saved outputs cleared; saved output is
runtime history, not Current acceptance evidence.

Configured Secret names are runtime inputs, not credentials stored in source.
Colab preserves the exact configured logical name. Local execution derives the
canonical `PROJECT_CODE` alias once at the authentication boundary for
supported suffixes such as `_GSHEET`, `_SHOPIFY_ACCESS_TOKEN`, and
`_SHOPIFY_TOKEN`; business and API call sites must not maintain per-project
Secret mappings.

## Logical Notebook Structure

Historical Runners may physically use more cells, but they should map to three logical sections.

For the GitHub-backed Edit__Core Runner, Config declares
`EXPECTED_MODULE_VERSION`; Load refreshes `main` and locates the canonical
`shopify_sync/edit_core.py`; Run imports or reloads that module, prints its
path and `MODULE_VERSION`, and fails closed unless the actual version exactly
matches Config. A versioned delivery name never replaces the canonical import
path. Current Edit__Core expects
`edit_core_product_status_v10_20260730`.

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

High-risk Apply Runners enforce the version contract twice:

1. immediately after loading the modules; and
2. immediately before calling Apply.

Both gates define expected versions in the current execution chain and compare
them with the loaded modules. For Generic Create Current, Apply `1.5.10`
requires Prepare `1.6.6`.

A clean Kernel is required for acceptance after a module or expected-version
change. Saved globals, imported modules, Notebook outputs, local `__pycache__`,
and `.runtime/ecom` copies are not evidence of Current. Acceptance starts from
the first cell and prints the repository, branch, commit SHA, module
`__file__`, module versions, and loaded function source before execution.

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

Generic Apply does not read or compare Preview as an execution gate. It
re-reads current Input, Defaults, `Cfg__Fields`, and `Cfg__Locations`, rebuilds
the plan, and reads `V_Product_Handle.Product Handle` once for local
target-existence comparison. `V_Product_Handle` is a user-created and
user-maintained Tab; Console Core requires only that exact Tab name and the
`Product Handle` header. No automatic synchronization Job, formula source, or
Product IDX upstream belongs to this Runtime contract. Prepare's
Input-internal `DUPLICATE_HANDLE` contract is distinct: Generic Create Input is Variant-grain,
`sys.product_key` defines a Product group, and only different Product groups
sharing one trimmed, case-insensitive Handle conflict.

Generic Apply Dry Run may perform current-state Shopify reads for accessible
Publications. Handle existence is checked from the Sheet snapshot, with zero
Shopify Handle lookup requests. Dry Run must not invoke Product, Publication,
Inventory, or Metafield mutation operations. Result and RunLog rows created as
dry-run evidence must report planned operations, zero Shopify business writes,
and the exact remaining Sheet side effects.

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

## APOLLO Shipping Profile Assignment Current

The stable Runner defaults to `DRY_RUN=True` and `CONFIRMED=False`. It requests
Shopify Admin GraphQL `2026-07`, requires at least `2026-07`, and fails closed
if `X-Shopify-API-Version` reports a different effective version. It also
fails closed when `shop.features.marketDrivenShipping` cannot be verified or
is enabled.

The required `Edit__ShippingProfileAssignments` Tab and its exact five-column
header must already exist. The Runner must not call `add_worksheet` or mutate
Sheet structure when the Tab is absent. READY Variants are automatically
processed in bounded API batches; a confirmed mutation is successful only
when per-Variant readback equals the requested target Delivery Profile GID.

`NO_CHANGE` and `rows_skipped` are distinct: `no_change_count` records
Variants already in the requested target state, while `rows_skipped` counts
only explicit skip outcomes. Validation errors are reported through
`error_count`; neither validation errors nor `NO_CHANGE` inflate
`rows_skipped`. The existing 18-column RunLog header remains unchanged.

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

## Generic Create Concurrency and Sheets Writes

Generic Apply may execute new Product groups with bounded Product-level
concurrency. A Product worker performs Shopify Product and Publication
operations and returns a structured outcome; it does not write Google Sheets.
Result and RunLog writes are serialized on the main thread through one writer.
With `STOP_ON_FIRST_ERROR=False`, one Product failure does not prevent later
Product groups from being submitted.

Quota-safe Result behavior includes:

- pre-size the Result worksheet once using expected output rows;
- buffer completed Product outcomes;
- require both a completed-Product threshold and a minimum time interval for
  intermediate flushes;
- force the final remaining buffer to write;
- retry Google Sheets HTTP 429, 500, 502, 503, and 504;
- honor `Retry-After` and use bounded exponential backoff;
- retain the buffer during failed attempts and clear it only after success;
- apply the same retry protection to final and failure RunLog writes.

## Failure and Recovery

- Validation failure occurs before writes whenever complete-plan validation is possible.
- Batch failures should be isolated to the responsible owner and field, including recursive splitting where appropriate.
- Retries and rate-limit waits are visible in progress and final metadata.
- Fallback/degraded behavior is explicit and never silently changes data meaning.
- Product-creation partial failure leaves the product DRAFT and records recovery information.
- The final result identifies safe resume conditions and does not describe partial work as complete.
- Shopify mutations and Google Sheets Result/RunLog evidence are not a single
  transaction. If evidence writing fails after Shopify work, do not rerun from
  a stale Handle snapshot. Have the operator update
  `V_Product_Handle.Product Handle` through the user-owned manual maintenance
  process, then rerun so existing Handles are skipped.
