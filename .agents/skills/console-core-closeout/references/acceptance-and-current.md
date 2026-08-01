# Acceptance and Current

## Independent Dimensions

Assess separately:

1. `CURRENT_IDENTITY`: which task-relevant artifact or revision is authoritative.
2. `TASK_ACCEPTANCE`: whether the bounded task reached its required level.
3. `PROJECT_HEALTH`: broader risks, Pending work, validation gaps, and boundary conflicts.
4. `RELEASE_READINESS`: whether release, publish, freeze, or milestone requirements are satisfied.

One dimension must not overwrite another. Under `TASK_ONLY`, a project-wide gap blocks closeout only when it violates the task scope or acceptance target.

## Console Core Current Dimensions

Report only task-relevant detail for:

```text
PYTHON_CURRENT
RUNNER_CURRENT
DOCUMENTATION_CURRENT
EXPECTED_VERSION_ALIGNMENT
GIT_CURRENT
```

Python Git Current may be current while the Runner expects an older version. A Current Runner may lack exact-current Runtime acceptance. Current identity never implies production readiness or user acceptance.

Use this authority order when sources conflict:

1. explicit current user confirmation for the task-relevant identity;
2. `PROJECT.md` for project Current and routing;
3. Git `main` and stable paths for Python and formal documentation;
4. task-relevant authoritative owner documents;
5. Runner operational facts for Runner identity only;
6. logs, outputs, caches, archives, attachments, and historical copies.

A lower source cannot silently overturn a higher owner. Verify user-provided identity claims against repository path, branch, commit, and bytes where available.

## Current Asset Hygiene

Use a disposition only when a task-relevant ambiguity actually exists:

```text
KEEP_CURRENT
UPDATE_IN_PLACE
ARCHIVE_WITH_REASON
DELETE_OBSOLETE
NEEDS_CONFIRMATION
```

Ordinary history belongs in Git. Do not create parallel Current files named `final`, `new`, `fixed`, `copy`, dated, numbered, or similar. Do not delete, move, merge, archive, or retire an asset solely because it is old, empty, similarly named, or apparently duplicated. Require call-site, behavior, authority, and preservation evidence plus authorization.

Runtime Cache and saved Notebook output are not alternate Current assets.

## Acceptance Ladder

Use only the strongest proven level:

```text
DESIGNED
→ IMPLEMENTED
→ STATICALLY_VERIFIED
→ TEST_VERIFIED
→ DRY_RUN_VERIFIED
→ LIVE_RUN_VERIFIED
→ BUSINESS_RECONCILED
→ ACCEPTED
→ FROZEN
```

The ladder is descriptive, not a requirement to perform every higher-cost step. Dry Run does not prove Live behavior. Live evidence does not prove business reconciliation or acceptance.

## Evidence Boundary

Classify evidence:

- `EXACT_CURRENT`: tied to the exact task-relevant Current bytes, commit, Runner identity, and required provenance.
- `PREVIOUS_BYTES`: valid evidence for an earlier commit, hash, module version, or Runner bytes.
- `EVIDENCE_GAP`: required proof is absent or cannot be bound to the exact Current.

Historical Live Run evidence cannot prove changed bytes. Conversely, do not repeat a Live Apply merely to improve the label when existing evidence and static reasoning already support the required decision without new external risk.

Preview, Result, RunLog, Notebook output, and runtime provenance support claims but do not define Current authority.

## Finding Classification

- `ACCEPTABLE_WARNING`: bounded; does not break correctness, authority, traceability, permission, or business meaning.
- `EVIDENCE_GAP`: missing or mismatched proof; not automatically a defect.
- `TASK_BLOCKER`: prevents safe completion of the task scope or required acceptance target.
- `PROJECT_HEALTH_RISK`: affects broader health but does not automatically block the scoped task.
- `RELEASE_BLOCKER`: prevents a formal release, freeze, milestone, or publication.

Stop mutation when a gap would require inventing a contract, choosing between conflicting Current identities, or modifying an unapproved repository or external system.
