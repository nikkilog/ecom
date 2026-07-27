# Console Core

```text
PROJECT_ID: CONSOLE_CORE
PROJECT_NAME: Console Core
REPOSITORY_PATH: /Users/nikki/Documents/AI_Workspace/Projects/Console_Core
LIFECYCLE: ONBOARDING
MAINTENANCE_PROFILE: COMPLEX
CURRENT_AUTHORITY: This PROJECT.md for project state and routing; the Console_Core Git main branch for Python, requirements, tests, and formal project documentation
CLOSEOUT_METHOD: $project-closeout
```

## Project Purpose

Console Core is a first-level project that provides reusable commerce-operations infrastructure for multiple Shopify projects. It centralizes site and account routing, non-sensitive Secret names, Sheet and Job registration, schema governance, Shopify exports, preparation of human-editable data, controlled Shopify writes and product creation, execution feedback, and RunLog governance.

PBS / plumbingsell is a primary consumer and validation project, not the identity or boundary of Console Core.

## Lifecycle and Current Phase

The project is in `ONBOARDING`.

The current phase establishes governance and authority around an existing codebase and Notebook Runner workspace. It does not certify existing modules as production-ready and does not authorize Shopify writes, Google resource changes, publishing, or cloud synchronization.

## Current Scope

- Console, Site, Account, Secret-name, Sheet, and Job routing.
- `Cfg__Fields`, Metaobject Definition, and related schema registration.
- Shopify export, pre-edit transformation, edit/apply, inventory, and product-creation modules.
- The contract between thin Colab Notebook Runners and Git-hosted Python modules.
- Preview, confirmation gates, progress, result, warning/error, and RunLog governance.

Detailed package status is owned by `docs/CODE_MAP.md`. Data stages and ownership are defined in `docs/DATA_LINEAGE.md`. Runtime behavior is defined in `docs/RUNTIME_CONTRACT.md`.

## Current Authority

- This file is authoritative for project identity, lifecycle, current phase, boundary, maintenance map, and the unique next action.
- The `Console_Core` Git repository on `main` is authoritative for Python source, requirements, future tests, and formal project documentation.
- `/Users/nikki/Documents/AI_Workspace/Projects/Console_Core_Colab` is a Notebook Runner / operational workspace. It is not authoritative for Python source or Git history.
- `Console_Core_Colab/PBS/07. create_product/generic/.runtime/ecom` is a disposable Runtime Cache. It may be rebuilt from Git and is not a second source repository or an independent project.
- Google Sheets, the central Console workbook, Google Drive, Colab Runtime state, and Shopify are operational resources. They do not silently override local Git Current.

## Current Status

- The existing repository is connected to `https://github.com/nikkilog/ecom.git`; the remote name does not match the Console Core project identity. This onboarding records the mismatch but does not change the remote.
- Existing Python and Notebook assets have been preserved in place.
- Existing modules have not received end-to-end production validation as part of onboarding.
- `shopify_create` remains under design and construction.
- Historical, empty, overlapping, and boundary-conflict candidates are recorded in `docs/CODE_MAP.md`; none have been deleted or retired.

## Explicitly Out of Scope

- Commerce Analytics Hub collection, analytics, validation, reporting, Overview, and Google Ads responsibilities.
- Moving analytics or Overview routes currently mixed into the Console.
- Treating PBS-specific configuration, including size ordering, as universal Console Core facts.
- Modifying Python, Notebook, requirements, Runtime Cache, Git remote, Google Drive, Google Sheets, Shopify, or Secret material during governance onboarding.
- Claiming a module is production-ready without end-to-end evidence.

## Open Questions and Known Overlap

- `COS / Commerce Operations System` exists as a planned Workspace entry. Whether it is a former identity of Console Core or a separate future project is deferred. This project does not modify, reuse, overwrite, or register `COS`.
- `business_overview` has a boundary-conflict risk with Commerce Analytics Hub.
- The long-term authority split between the two Metaobject Definition synchronizers and between historical export/config implementations requires call-site and behavior evidence.
- The GitHub repository/remote identity remains `nikkilog/ecom.git`; any rename is a separate user-approved operation.

## Maintenance Map

| Responsibility | Authority |
|---|---|
| Identity, lifecycle, current phase, scope, Current, next action | `PROJECT.md` |
| Mandatory agent operation and safety rules | `AGENTS.md` |
| Human-oriented project and directory entry | `README.md` |
| Package/module inventory and evidence status | `docs/CODE_MAP.md` |
| Data stages, ownership, grain, and transitions | `docs/DATA_LINEAGE.md` |
| Notebook/Python loading, progress, safety, and result contract | `docs/RUNTIME_CONTRACT.md` |
| Python implementation and requirements | Git `main` |
| Notebook Runner execution surface | sibling `Console_Core_Colab` workspace |
| Ordinary history | Git |

## Next Action

Perform a read-only call-site and contract validation of the existing Python modules and Notebook Runners, then produce a bounded plan that identifies authoritative implementations, incomplete wiring, test priorities, and safe retirement candidates without modifying operational code.
