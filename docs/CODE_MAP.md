# Console Core Code Map

## Status Vocabulary

| Status | Meaning |
|---|---|
| `CURRENT_AUTHORITY` | The authoritative implementation or owner for its declared responsibility, without implying production validation |
| `ACTIVE_BUT_UNVALIDATED` | Substantive current code exists, but end-to-end production behavior has not been verified |
| `EXPERIMENTAL` | Design or implementation is still evolving and must not be treated as the final common contract |
| `HISTORICAL_CANDIDATE` | Appears superseded, duplicated, or misleading; requires call-site and behavior evidence before retirement |
| `EMPTY_PLACEHOLDER` | Empty or effectively empty asset with no demonstrated behavior |
| `BOUNDARY_CONFLICT` | Its responsibility may belong to another first-level project |

No package is labeled production-ready by this map.

## Authority Map

| Area | Status | Current responsibility and evidence boundary |
|---|---|---|
| Git `main` Python source | `CURRENT_AUTHORITY` | Formal Python source authority; individual module behavior remains subject to validation |
| `PROJECT.md` | `CURRENT_AUTHORITY` | Project identity, Current, phase, boundary, maintenance map, and next action |
| `Console_Core_Colab` | `ACTIVE_BUT_UNVALIDATED` | Notebook Runner / operational workspace; not Python or Git-history authority |
| `.runtime/ecom` | `HISTORICAL_CANDIDATE` | Disposable runtime clone/cache; current use is allowed, but it is never a second authority |

## Package Inventory

### `shopify_export`

Overall status: `ACTIVE_BUT_UNVALIDATED`

Known intended responsibilities:

- `3_1_1_export_product_idx_tables.py`
  (`shopify_export.3_1_1_export_product_idx_tables`): build Product and Variant
  index snapshots in `IDX__Products` and `IDX__Variants`. The 3.1.1 pilot was
  live-validated in Local and Colab at Git commit
  `eb5a987ee2363b4e28ef594aff84bf38a7de21df`, with both Runtimes loading module
  version `2026-05-24-calc-fields-in-fetch-v2` and reporting reconciled business
  results. This is bounded validation of the 3.1.1 pilot, not validation of the
  whole `shopify_export` package or a production-readiness claim.
- `dl_values_long.py`: fetch configured dependencies not covered by index tables.
- `build_product_views.py`: build operational views from index, long values, and configuration without Shopify access.
- `export_collection_views.py`: serve multiple collection views from a shared Shopify pull.
- `export_customers.py`: serve multiple customer views from a shared Shopify pull.
- `export_metaobject_entries.py`: export Metaobject Entries with dynamic list/reference expansion.
- `export_mr_example.py`: expand Metaobject Reference examples.
- `export_mr_validate.py`: validate reference completeness and targets.

Candidates requiring evidence:

- `export_collections.py`: `HISTORICAL_CANDIDATE`. Its implementation substantially overlaps configuration-field behavior and its filename may not describe its actual responsibility.
- `export_metaobject_defs.py`: `HISTORICAL_CANDIDATE`. It overlaps the Metaobject Definition synchronizer in `shopify_ops`.

Neither candidate may be deleted until Notebook/module call sites, input/output contracts, and behavior differences are verified.

### `shopify_pre_edit`

Overall status: `ACTIVE_BUT_UNVALIDATED`

- `wide_to_long.py`
- `wide_to_metafieldblocks.py`
- `entries_create_wide_to_long.py`
- `entries_update_wide_to_long.py`
- `wide_to_media_edits.py`

Intended boundary: transform human-editable wide inputs into validated machine-facing long, block, skeleton, or edit contracts. It should not directly write Shopify.

Validation priorities include owner authority, exclusion of `calc.*` and display-only fields, ordered lists/blocks, auxiliary-column handling, zero-output anomalies, deterministic duplicate rules, and planned-versus-written reconciliation.

### `shopify_sync`

Overall status: `ACTIVE_BUT_UNVALIDATED`

Substantive modules:

- `edit_metafields.py`
- `edit_metafieldblocks.py`
- `edit_core.py`: Current module version
  `edit_core_product_status_v10_20260730`. Its generic Product core contract
  includes `core.status` with `SET` for `ACTIVE`, `DRAFT`, or `ARCHIVED`.
  This records the authoritative implementation and focused local contract
  tests; it does not establish live end-to-end validation.
- `edit_entries_create.py`
- `edit_entries_update.py`
- `edit_refs.py`
- `edit_theme_template.py`

Placeholders:

- `inventory_sync.py`: `EMPTY_PLACEHOLDER` — zero bytes.
- `init.py`: `EMPTY_PLACEHOLDER` — zero bytes and not a package `__init__.py`.

The edit modules require evidence for complete-plan validation, current-state reads, Preview fidelity, dual write gates, ID-based owner resolution, type-specific CLEAR behavior, reference difference calculation, ordered values, batch-failure isolation, result accuracy, and RunLog phase fidelity.

`edit_metafields.py` defines metafield `CLEAR` as deleting/unsetting the
metafield by immutable owner ID, namespace, and key. It does not model CLEAR
as an empty-string SET.

### `shopify_ops`

Overall status: `ACTIVE_BUT_UNVALIDATED`

- `config_fields.py`: current substantive field-registry synchronizer candidate.
- `config_metaobject_defs.py`: current substantive Metaobject Definition synchronizer candidate.
- `init.py`: `EMPTY_PLACEHOLDER` — zero bytes and not a package `__init__.py`.

`config_fields` and `config_metaobject_defs` have historical responsibility overlap. Do not merge them until their authoritative ownership, call sites, preservation of human-governed columns, Preview/diff behavior, and RunLog contracts are verified.

### `shopify_create`

Overall status: `EXPERIMENTAL`

- `generic_prepare.py`: Current module version `1.6.6`. Generic Create Input is
  Variant-grain and `sys.product_key` defines the Product group. Multiple
  Variants in one Product group may share one normalized `core.handle`.
  Prepare reports only `DUPLICATE_HANDLE`, meaning the same trimmed,
  case-insensitive Handle is used by more than one distinct Product group.
  SKU, Barcode, Variant Key, Product-field differences, dimensions, and Draft
  publication intent are not Preview warning or error checks.
- `generic_apply.py`: Current module version `1.5.10`; it requires
  `generic_prepare` `1.6.6`. It does not use Preview as an execution gate. It
  re-reads current Input, Defaults, `Cfg__Fields`, and `Cfg__Locations`,
  rebuilds the execution plan, and reads
  `V_Product_Handle.Product Handle` once as the target-existence snapshot.
  Apply compares only normalized Handle in memory. Existing Handles become
  `SKIPPED_HANDLE_EXISTS`, produce Result evidence, and cause no Shopify
  Handle lookup or Product mutation. New Handles are processed by bounded
  Product workers. Result and RunLog Google Sheets writes remain on the main
  thread through a single quota-safe writer with batching, minimum flush
  interval, pre-sizing, and retry protection.

The area is still under design and construction. Existing generic, legacy, wholesale, and SPU preparation paths do not yet establish one final common product-creation contract.

Current evidence level:
`LIVE_MULTI_VARIANT_PRODUCT_CREATION_VALIDATED`. Generic Prepare `1.6.6` has
completed a live two-row, one-Product, two-Variant validation with `READY`,
zero warnings, and zero errors. Generic Apply `1.5.10` has completed a live
creation of one DRAFT Product with two Variants, associated the Product with
accessible Publications, and wrote two Result rows successfully. The
`V_Product_Handle.Product Handle` snapshot was read and compared locally, with
zero Shopify Handle lookup requests. The quota-safe Google Sheets writer
succeeded for this single-Product run.

This evidence does not establish large-batch concurrency or quota stress
validation. It also does not certify every edge Schema, Publication,
partial-failure, retry-exhaustion, or recovery path through live execution.

Required safety direction includes Prepare → Review → Apply, `APPROVED` filtering, Apply-time configuration and snapshot checks, append-only result history with stable idempotency keys, DRAFT-first creation, partial-failure retention in DRAFT, and explicit recovery/reconciliation behavior.

`core.handle` is the Shopify Product identity for Generic Create.
`sys.product_key` is the Input Product-group boundary, not the target-system
existence key. `sys.variant_key`, SKU, and Barcode remain trace or business
values and are not duplicate-blocking identities. A Draft Product may be
associated with accessible Publications, but customer availability remains
controlled by Product status.

### `shopify_setup`

Overall status: `ACTIVE_BUT_UNVALIDATED`

- `sync_locations.py`: Current module version `2.3.1`. It resolves the active
  project through the Workspace Project Registry, then uses the selected
  project Console Core route and project-specific Secret name to synchronize
  Shopify Locations into `Cfg__Locations`.

`Cfg__Locations` system-managed fields are `site_code`, `location_name`,
`location_gid`, `province_code`, `active`, and `synced_at`. Human-managed
fields are `location_code`, `is_default`, and `notes`; synchronization
preserves those human values. End-to-end validation and confirmed real-write
evidence remain outstanding.

### `business_overview`

Overall status: `BOUNDARY_CONFLICT`

- `build_overview_refs.py` is substantive current code.
- Its Overview responsibility may belong to Commerce Analytics Hub rather than Console Core.

Preserve it in place until consumers, configuration ownership, and the CAH boundary are confirmed. Do not interpret its current location as proof of long-term Console Core ownership.

### `shopify_analytics`

Overall status: `EMPTY_PLACEHOLDER`

- `export_order_lines.py`: zero bytes.
- `export_orders.py`: effectively empty.
- `init.py`: zero bytes and not a package `__init__.py`.

The package is an empty shell, not an implemented analytics capability. Analytics responsibility also risks overlap with Commerce Analytics Hub.

### `image_tools`

Status: not present in Current.

Git history shows that this directory was removed before onboarding. It must not be listed as a current package or recreated without a new, approved responsibility.

## Notebook Runner Map

The sibling `Console_Core_Colab/PBS` workspace currently groups Runners for:

- setup;
- pre-edit;
- edit;
- configuration;
- product and other exports;
- customers, orders, shipping, and inventory directory placeholders;
- product creation, including legacy, wholesale, generic, and SPU-preparation paths.

Notebook presence or absence does not prove Python capability. Notebook filenames, saved output, job names, module versions, and Cell parameters require contract validation against actual imported functions.

The APOLLO Generic Create Runners are operational assets in
`Console_Core_Colab`, not formal Python Current. RichText generation uses
non-empty current Input HTML first and may use existing generated RichText
only when Input is blank. Runtime copies under `.runtime/ecom` are disposable
execution caches and may lag Git Current.

The stable APOLLO Shipping Profile Assignment Runner is
`APOLLO/05_5. export_shipping/edit_shipping_profile_assignments.ipynb` in the
sibling Notebook workspace. Status: `ACTIVE_BUT_UNVALIDATED`. It is a
Colab-only Controller/runtime implementation, not a Python module and not a
Shipping Suite refactor. The prior v1 remains historical evidence and has not
been deleted or promoted. The Current Runner is bounded to exact Variant ID →
Delivery Profile GID instructions and preserves the existing 18-column
RunLog schema.

## Known Identity and Boundary Issues

- The Git remote remains `nikkilog/ecom.git`, which does not match the Console Core project identity. It is recorded, not changed.
- `COS / Commerce Operations System` may overlap conceptually with Console Core; identity resolution is deferred.
- Analytics and Overview routes historically mixed into the Console are not automatically part of Console Core's long-term boundary.
- No candidate is eligible for retirement solely because it is empty, old, similarly named, or partially duplicated.
