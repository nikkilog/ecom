# Console Core

Console Core is reusable commerce-operations infrastructure for multiple Shopify projects. It provides central routing, schema and configuration governance, Shopify exports, human-edit preparation, controlled writes and product creation, and shared execution-safety conventions.

It is a first-level project alongside Commerce Analytics Hub. PBS / plumbingsell is an important consumer and validation project, not the identity of Console Core.

## Start Here

- Project Current, scope, phase, and next action: `PROJECT.md`
- Mandatory operation and safety rules: `AGENTS.md`
- Package and asset status: `docs/CODE_MAP.md`
- Pipeline stages and data ownership: `docs/DATA_LINEAGE.md`
- Colab/Python runtime and result contract: `docs/RUNTIME_CONTRACT.md`

## Authority and Workspaces

```text
Console_Core Git main
= Python source
= requirements and future tests
= formal project documentation
= Git history

Console_Core_Colab
= Notebook Runner / operational workspace
!= Python source authority
!= Git history authority

Console_Core_Colab/.../generic/.runtime/ecom
= disposable Runtime Cache
!= independent project
!= second source repository
```

The current remote is `https://github.com/nikkilog/ecom.git`. That remote identity predates the formal Console Core project name and is intentionally unchanged by onboarding.

## Current Package Areas

- `shopify_export`: Shopify fact export and operational-view construction.
- `shopify_pre_edit`: conversion of human-editable wide inputs into machine contracts.
- `shopify_sync`: controlled Shopify edit/apply operations.
- `shopify_ops`: configuration and schema synchronization.
- `shopify_create`: product-creation design and implementation work.
- `shopify_setup`: site/setup synchronization utilities.
- `business_overview`: current boundary-conflict candidate with Commerce Analytics Hub.
- `shopify_analytics`: currently an effectively empty shell.

Directory presence does not certify completion or production readiness. See `docs/CODE_MAP.md` for evidence status.

## Runtime Shape

Notebook Runners should remain thin:

1. Config — site, routes, non-sensitive Secret names, Sheets, and safety switches.
2. Load — authenticate, install dependencies, fetch the exact Git revision, clear stale imports, load modules, and print provenance.
3. Run — invoke Python, show Preview and progress, then return structured results.

Real external writes require both `DRY_RUN = False` and `CONFIRMED = True`. Full details are in `docs/RUNTIME_CONTRACT.md`.

## Public Registry Resolution

Install the formal shared package from the canonical Local checkout:

```bash
python3 -m pip install -e /Users/nikki/Documents/AI_Workspace/Projects/Console_Core
```

Import the target-resource resolver with:

```python
from console_core import resolve_sheet_resource
```

`resolve_sheet_resource` is a pure row-selection and validation boundary. The
caller supplies already-loaded Workspace Project Registry and `Cfg__Sites`
rows. Console Core does not resolve caller execution identity, credentials, or
Secrets through this API.

For Colab, install from the repository only when the command pins a concrete,
user-authorized commit revision rather than an unpinned branch.

Real Colab installation and Registry access require separate authorization.

## Development Status

Console Core is being governed around an existing implementation. Existing packages have not been certified end-to-end by this onboarding. Historical overlaps, placeholders, incomplete paths, and boundary questions are preserved for evidence-based review.
