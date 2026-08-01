# Authority and Scope

## Authority Boundary

```text
Console_Core Git main
= Python source authority
= requirements and future tests
= formal project documentation
= Git history authority

Console_Core_Colab
= Notebook Runner and operational workspace
!= Python source authority
!= Git history authority

.runtime/ecom
= disposable Runtime Cache reconstructed from Git
!= independent project
!= second repository
!= second Current
```

Google Sheets, Shopify, Google Drive, Colab Runtime state, and the central Console workbook are operational resources. They may own current operational facts within their systems, but they do not silently override local Git Python, documentation, Notebook, or project Current.

Do not treat the sibling Runner workspace as part of the primary repository or modify it merely because related Python changed. Do not treat cached or loaded module bytes as Current without provenance.

## Project Boundary

Console Core owns reusable commerce-operations routing, configuration and schema governance, operational exports and transformations, controlled Shopify writes and creation, and their runtime/result governance.

It does not acquire analytics, reporting, Overview, or another first-level project's responsibility merely because historical files or Console routes exist. Preserve boundary-conflict candidates until evidence and approval support reclassification.

## Document Owners

- `PROJECT.md`: identity, boundary, lifecycle/current phase, project Current, maintenance map, and single `NEXT_ACTION`.
- `AGENTS.md`: mandatory repository-wide agent operation and safety constraints.
- `README.md`: navigation and usage orientation; never project-state authority.
- `docs/CODE_MAP.md`: package/module responsibility, implementation/status evidence, call-site or overlap evidence, and boundary conflicts.
- `docs/DATA_LINEAGE.md`: stage, entity/grain, stable key, ownership, transformation, and handoff.
- `docs/RUNTIME_CONTRACT.md`: Notebook/Python loading, provenance, stale-module prevention, progress, Preview, write gates, Result, and RunLog behavior.
- Git `main`: Python implementation, requirements, future tests, formal documentation, and ordinary history.
- `Console_Core_Colab`: operational Runner entry points, not formal Python or Git history.

Update only the owner whose relationship changed. Do not duplicate detailed Current or module status across owners. A normal bugfix does not require touching every document.

## Repository Scope

Default to one primary repository per closeout. Verify the exact root and keep sibling or first-level repositories outside the write package unless the user separately authorizes a dedicated task.

Never infer authorization to:

- modify `Console_Core_Colab`;
- change the `nikkilog/ecom.git` remote or repository identity;
- edit Workspace_Control, Personal_Knowledge_Base, or another project;
- write Shopify, Sheets, Drive, Colab, or other cloud resources;
- expose or move Secret material;
- commit, merge, push, publish, release, or cloud-sync.

Route cross-repository consequences through complete downstream packages rather than direct edits.
