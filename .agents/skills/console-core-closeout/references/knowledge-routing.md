# Knowledge Routing

## One Detailed Owner

Give each durable fact one detailed home. Update an existing owner in place and summarize elsewhere only when navigation requires it.

- `PROJECT.md`: identity, boundary, lifecycle/current phase, Current, maintenance map, and single `NEXT_ACTION`.
- `AGENTS.md`: mandatory repository-wide agent rules and safety constraints.
- `README.md`: navigation and usage orientation, not project status.
- `docs/CODE_MAP.md`: package/module responsibility, implementation/status evidence, overlap, call-site, and boundary-conflict evidence.
- `docs/DATA_LINEAGE.md`: stage, grain, stable key, owner, transformation, and handoff.
- `docs/RUNTIME_CONTRACT.md`: Notebook/Python loading, provenance, stale protection, progress, Preview, write gates, Result, and RunLog.
- task-relevant tests, results, and logs: proof and diagnostics, not Current authority.
- `NO_ACTION`: no owned relationship changed or the existing owner already states it correctly.

Do not update every document for an ordinary bugfix. Do not create a new broad summary, status map, Pitfall file, or parallel Current when an owner already exists.

## Project-Specific Pitfalls

Keep Console Core-specific operating facts, module behavior, versions, Current, task progress, and Next Action inside Console Core. Place a real repeated Pitfall in the owner closest to its continuing responsibility.

A durable Pitfall should compactly preserve:

```text
symptom
root cause
wrong handling
correct handling
prevention rule
```

Do not promote a one-off observation, inference, or unverified concern into a permanent rule.

## Workspace_Control Routing

Set `WORKSPACE_CONTROL_SYNC_REQUIRED: YES` only when closeout establishes a change to:

- project identity or Project ID relationship;
- repository path;
- lifecycle;
- maintenance profile;
- formal Current authority;
- project purpose or first-level boundary;
- cross-project routing;
- registered Closeout Method.

Do not route bugfixes, internal versions, run IDs, task evidence, module status, or project Next Action to Workspace_Control.

When `YES`, output a complete `$workspace-control-sync` call package containing verified identity/path/lifecycle/profile/authority/Closeout Method changes, source-project evidence, and explicit exclusions. Do not edit Workspace_Control from this Skill.

## PKB Routing

Set `PKB_SYNC_REQUIRED: YES` only for:

- durable cross-task personal preferences; or
- reusable, verified cross-project Pitfalls.

Exclude project state, module state, versions, identifiers, Current files, business rules, readable repository facts, and Next Action.

When `YES`, output a complete `$pkb-knowledge-absorption` package containing:

```text
PACKAGE_SCHEMA_VERSION: 1
SOURCE_MODE: PROJECT_CLOSEOUT_PACKAGE
PKB_SYNC_REQUIRED: YES
SOURCE_PROJECT: Console Core
SOURCE_SUMMARY: <verified durable candidates>
PROJECT_ONLY_EXCLUSIONS: <facts that must remain in Console Core>
```

The package transports candidates; it is not independent evidence. Do not edit Personal_Knowledge_Base from this Skill.

## Project Structure Upgrade Routing

Recommend a structure upgrade only when Console Core's existing authoritative owners can no longer represent a real continuing responsibility clearly. Do not recommend one because another complex project has more documents.

When `PROJECT_STRUCTURE_UPGRADE_RECOMMENDED: YES`, output a complete `$project-onboarding` package including:

```text
PROJECT_ACTION: UPGRADE_EXISTING
PROJECT_PROFILE: COMPLEX
TARGET_PROJECT_PATH: /Users/nikki/Documents/AI_Workspace/Projects/Console_Core
```

Do not perform the upgrade inside closeout.

## Complete Package Rule

A boolean is not a handoff. Every downstream `YES` must include a directly usable package with verified source facts, requested action, bounded target, required inputs, exclusions, and authorization boundaries. Use `N/A` for the package when the answer is `NO`.

Never call `$console-core-closeout` from its own downstream output.
