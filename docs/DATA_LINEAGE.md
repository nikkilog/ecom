# Console Core Data Lineage

## Canonical Operational Flow

```text
Console / Config
→ Export
→ Pre Edit
→ Review
→ Edit / Apply
→ Shopify
→ Result / RunLog
```

Each transition has a separate responsibility. A downstream stage must not silently reinterpret planned counts, owner identity, field meaning, or side effects from an upstream stage.

## Stage Ownership

### 1. Console / Config

Purpose:

- route by `site_code`, label, Sheet, Job, account parameters, and non-sensitive Secret names;
- register field and Metaobject schema;
- hold site-specific configuration and business rules without embedding Secret values.

Key resources include the central Console, `Cfg__Sites`, `Cfg__colab&sheets`, `Cfg__account_id`, `Cfg__Fields`, and `Cfg__MetaobjectDefs`.

Workspace-aware operations use two-stage project routing:

```text
WORKSPACE_GSHEET
→ Workspace Project Registry / Cfg__Projects
→ active PROJECT_CODE row
→ project Console Core URL + project Google Secret name
→ project Cfg__account_id
→ project operation
```

The Workspace credential is limited to resolving the registry route. The
project-selected credential authenticates the project operation.

`Cfg__Fields` canonical logical key:

```text
entity_type + field_key
```

Automated synchronization may restore Shopify facts and system-derived columns, but it must preserve human governance such as purpose, sequence, lookup/join semantics, concepts, applicability, notes, and review status.

`Cfg__Locations` grain is one registered Shopify Location per `site_code` and
`location_gid`. System synchronization owns `site_code`, `location_name`,
`location_gid`, `province_code`, `active`, and `synced_at`. Operators own
`location_code`, `is_default`, and `notes`. Generic Create consumes an active
`location_code` as its stable human-facing routing key and resolves the
immutable Location GID before execution.

Known non-core or site-specific content, such as analytics/Overview routes and PBS size ordering, must not be promoted into universal Console Core facts merely because it is present in the Console.

### 2. Export

Purpose:

- read Shopify facts;
- create canonical snapshots;
- build operational views from configured dependencies.

Preferred separation:

```text
Shopify fact read
→ canonical IDX / DL snapshot
→ configured operational view
```

Export does not own human edits or Shopify writes. Schema should determine required queries, and one Shopify pull should serve multiple compatible views where practical.

Typical grain and keys:

| Dataset class | Expected grain | Stable identity |
|---|---|---|
| Product index | one product | product GID or numeric ID |
| Variant index | one variant | variant GID or numeric ID |
| Long value | one owner/field/value-or-position | owner ID + field key + deterministic position where applicable |
| Metaobject entry | one entry or expanded entry field | entry GID plus field/position |
| Operational view | configuration-defined | explicit view key; never inferred solely from row order |

Exact schemas remain subject to module contract validation.

### 3. Pre Edit

Purpose:

- convert human-editable wide data into validated Long, Blocks, Skeleton, or other machine contracts;
- normalize owner, field, data type, action, value, and order;
- reject or explicitly report unrecognized inputs.

Authority rules:

- In a single-owner Wide table, the owner column has highest authority.
- In a multi-owner Wide table, field configuration determines the owner type.
- Immutable owner IDs are preferred over mutable handles or SKUs.
- `calc.*`, owner-ID fields used only for routing, display-only fields, and auxiliary columns such as `__handle` do not become write operations.
- Lists and structured blocks retain order.
- Scalar duplicates require an explicit deterministic policy.
- Non-empty input producing zero planned rows is an error unless an explicit no-change rule applies.

Pre Edit does not own Shopify side effects.

### 4. Review

Purpose:

- human review of Handle, validation findings, images, product groups, generated plans, and approval state.

Review output must preserve a stable key so Apply can identify approved business objects without relying on row order. Approval is not a substitute for Apply-time validation against current configuration and Shopify state.

Generic Create Input is Variant-grain. `sys.product_key` defines the Product
group, so multiple Variant rows in one Product group normally share one
`core.handle`. Prepare normalizes Handle with trim plus case-insensitive
comparison and reports `DUPLICATE_HANDLE` only when different Product groups
reuse the same normalized Handle. This is an Input-internal grouping conflict;
it is not evidence that the Handle already exists in Shopify. Variant Key,
SKU, Barcode, Product-field differences, dimensions, and Draft publication
intent are not duplicate-Handle checks.

RichText transformation treats non-empty current Input HTML as authoritative.
Previously generated RichText HTML is fallback input only when the
corresponding current Input is blank; valid historical output must not
silently outrank corrected Input.

### 5. Edit / Apply

Canonical execution:

```text
load complete input
→ validate fields and actions
→ resolve all owners
→ read current Shopify facts
→ construct complete plan
→ Preview
→ enforce DRY_RUN / CONFIRMED gates
→ execute bounded API operations
```

Important distinctions:

- input rows;
- validated rows;
- business objects planned;
- API operations planned;
- API operations attempted;
- API operations succeeded or failed;
- rows/results actually written.

These counts are not interchangeable.

State-dependent operations must re-read current facts. Reference `LINK`, `UNLINK`, and `REPLACE_ALL` calculate differences from current values. `CLEAR` behavior is defined by field type and module. Variant updates prefer immutable IDs over the SKU being modified. Metaobject updates prefer `entry_gid`.

Generic Apply `1.5.10` requires Generic Prepare `1.6.6`. Apply does not read or
compare Preview as an execution gate. It re-reads current Input, Defaults,
`Cfg__Fields`, and `Cfg__Locations`, then rebuilds the execution plan from
current facts.

The current Handle-existence flow is:

```text
operator-maintained target Handle facts
→ create_generic / V_Product_Handle
→ Product Handle snapshot
→ Generic Apply reads the snapshot once
→ normalized in-memory Handle-set comparison
→ existing: SKIPPED_HANDLE_EXISTS
→ new: Shopify productSet / publication
→ Result / RunLog
```

`V_Product_Handle` is a user-created and user-maintained Tab. Console Core
depends only on the exact Tab name `V_Product_Handle` and the header
`Product Handle`; it does not own or track an automatic synchronization Job,
formula source, or Product IDX upstream for this snapshot. The project must
not describe this Tab as automatically generated or refreshed.

Apply locates `Product Handle` by header name rather than fixed column
position, ignores blank snapshot values, and silently deduplicates repeated
snapshot Handles after trim plus case-insensitive normalization. SKU, Barcode,
Product Key, Variant Key, Title, Product ID, and Variant ID are not queried or
compared. Shopify Handle lookup API requests are zero.

New Product groups execute through bounded Product workers. Workers perform
Shopify Product and Publication work only; Result and RunLog writes are
serialized on the main thread. `STOP_ON_FIRST_ERROR=False` permits later
Product groups to continue after an isolated failure.

Prepare and Apply Handle checks must not be conflated:

- Prepare detects one normalized Handle assigned to different Input Product
  groups.
- Apply detects a normalized Handle already present in the target-system
  snapshot.

Publication association is an execution relationship; it does not make a
Draft Product customer-visible.

### 6. Shopify

Shopify is the external operational system of record for current commerce objects. It is not the authority for local Python source, project documentation, or Notebook code.

External modifications require both:

```text
DRY_RUN = False
CONFIRMED = True
```

Product creation should remain DRAFT until all required steps succeed. Partial failure keeps the product DRAFT and records `PARTIAL_FAILED` or the equivalent structured failure state.

### 7. Result / RunLog

Purpose:

- record actual execution rather than planned intent;
- expose success, failure, skip, warning, error, retries, fallback, operation type, and trace information.

Results should use stable idempotency and trace keys, including `run_id` and business-object identity. Append history must verify the target header contract before writing.

RunLog phases must correspond to real side effects. A Preview or dry run must not label planned work as written, and any allowed log or mapping side effect must be disclosed explicitly.

Generic Apply pre-sizes Result for expected rows and flushes buffered outcomes
in batches only after both the count threshold and minimum interval are met;
the final remaining buffer is forced to write. Result and final/failure RunLog
writes retry HTTP 429, 500, 502, 503, and 504 with bounded exponential backoff
and `Retry-After` support. A Result buffer is cleared only after a successful
write.

Shopify effects and Google Sheets evidence are not one transaction. A Shopify
Product may exist even if a later Result or RunLog write remains unsuccessful
after retries. Before recovery or rerun, the operator must manually bring
`V_Product_Handle.Product Handle` up to date using the user-owned maintenance
process; the updated snapshot lets already-created Handles resolve to
`SKIPPED_HANDLE_EXISTS`. Console Core does not infer or automate that process.

## Cross-Stage Invariants

- Field meaning is owned by the schema/configuration contract, not by ad hoc Notebook code.
- Owner resolution prioritizes immutable IDs.
- Lists and structured values preserve order through every stage.
- Reference editing reads current values before difference calculation.
- Warnings are graded, aggregated, counted, and accompanied by limited examples.
- Batch failure is isolated to the responsible owner/field where possible.
- Quality gates occur before destructive or replacing output.
- Every external side effect is attributable to a phase, operation, owner, and result.
- Runtime provenance identifies the exact Git code used.

## Boundary With Commerce Analytics Hub

Commerce Analytics Hub owns commerce analytics collection, validation, reporting, Overview, and advertising-analysis chains. Console Core owns Shopify operations routing, configuration governance, export for operations, controlled write/create workflows, and their runtime governance.

Historical files or Console rows that cross this boundary remain evidence candidates. They are not moved or reclassified without explicit validation and approval.
