# -*- coding: utf-8 -*-
"""Apply READY Shopify Flexible Collection creation plans from Preview.

GitHub target: ``ecom/shopify_create/7_5_2_collection_create_apply.py``
Import path: ``shopify_create.7_5_2_collection_create_apply``

Execution contract
------------------
1. Rebuild the current plan from Input/Defaults/Shopify using Prepare logic.
2. Read Preview and require a matching READY row + plan_hash for every selected
   Collection. This prevents applying a stale Preview after Input changes.
3. Re-check handle existence immediately before creation.
4. DRY_RUN performs no Shopify writes.
5. Live writes require ``dry_run=False`` and ``confirmed=True``.
6. Create each Collection with ``collectionCreate(collection: ...)``.
7. When ``publish_all_channels=TRUE``, publish the new Collection to every
   accessible Shopify Publication with ``publishablePublish``.
8. Read back the created Collection and verify its handle, match type, and
   condition count before writing SUCCESS to Result.
9. Overwrite Result for this run with one row per selected Collection.
"""
from __future__ import annotations

import argparse
import importlib
import json
import sys
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

cp = importlib.import_module("shopify_create.7_5_1_collection_create_prepare")
gp = importlib.import_module("shopify_create.7_1_1_generic_product_prepare")
ga = importlib.import_module("shopify_create.7_1_2_generic_product_apply")

MODULE_VERSION = "2026-09-04-flexible-collection-create-v1"
MODULE_PATH = "shopify_create.7_5_2_collection_create_apply"
DEFAULT_JOB_NAME = "collection_create_apply"
EXPECTED_PREPARE_MODULE_VERSION = cp.MODULE_VERSION

RESULT_HEADERS = [
    "run_id",
    "applied_at",
    "site_code",
    "dry_run",
    "title",
    "handle",
    "match_type",
    "condition_count",
    "apply_status",
    "collection_gid",
    "source_gid",
    "publish_all_channels",
    "publications_planned",
    "publications_published",
    "publication_ids",
    "readback_condition_count",
    "readback_match_type",
    "message",
    "error_reason",
    "plan_hash",
    "shopify_admin_url",
    "storefront_url",
]

M_COLLECTION_CREATE = """
mutation CollectionCreate($collection: CollectionCreateInput!) {
  collectionCreate(collection: $collection) {
    collection {
      id
      title
      handle
      sources {
        __typename
        id
        title
      }
    }
    userErrors {
      field
      message
    }
  }
}
"""

Q_COLLECTION_READBACK = """
query CollectionReadback($id: ID!) {
  collection(id: $id) {
    id
    title
    handle
    sources {
      __typename
      id
      title
      ... on CollectionConditionsSource {
        targetType
        inclusion {
          matchType
          conditions {
            __typename
            id
            ... on CollectionSourceInclusionConditionProductVendor {
              vendorRelation: relation
              vendorValues: values
              matchType
            }
            ... on CollectionSourceInclusionConditionProductType {
              typeRelation: relation
              typeValues: values
              matchType
            }
            ... on CollectionSourceInclusionConditionProductStatus {
              statusRelation: relation
              statusValues: values
              matchType
            }
            ... on CollectionSourceInclusionConditionProductTag {
              tagRelation: relation
              tagValues: values
              matchType
            }
            ... on CollectionSourceInclusionConditionMetafieldString {
              stringRelation: relation
              stringValues: values
              matchType
              definition {
                id
                namespace
                key
                type { name }
              }
            }
            ... on CollectionSourceInclusionConditionMetafieldStringList {
              listRelation: relation
              listValues: values
              matchType
              definition {
                id
                namespace
                key
                type { name }
              }
            }
          }
        }
      }
    }
  }
}
"""


def _read_preview(values: Sequence[Sequence[Any]]) -> Dict[str, Dict[str, str]]:
    if not values:
        raise ValueError("Preview is empty. Run collection_create_prepare first.")
    headers = [cp._normalize_header(v) for v in values[0]]
    positions = {name: idx for idx, name in enumerate(headers) if name}
    required = {
        "handle",
        "status",
        "plan_hash",
        "payload_json",
        "publish_all_channels",
    }
    missing = sorted(required - set(positions))
    if missing:
        raise ValueError(f"Preview missing required columns: {missing}")

    result: Dict[str, Dict[str, str]] = {}
    for source_row, raw in enumerate(values[1:], start=2):
        padded = list(raw) + [""] * max(0, len(headers) - len(raw))
        rec = {name: gp._safe_str(padded[idx]) for name, idx in positions.items()}
        handle = cp._normalize_handle(rec.get("handle"))
        if not handle:
            continue
        if handle in result:
            raise ValueError(f"Preview has duplicate handle={handle!r}.")
        rec["source_row"] = str(source_row)
        result[handle] = rec
    if not result:
        raise ValueError("Preview contains no Collection rows.")
    return result


def _select_handles(
    *,
    current_plans: Sequence[Mapping[str, Any]],
    preview_rows: Mapping[str, Mapping[str, str]],
    only_handles: Optional[Sequence[str]],
    apply_all_ready: bool,
    max_collections: Optional[int],
) -> List[str]:
    ready = [
        gp._safe_str(plan.get("handle"))
        for plan in current_plans
        if plan.get("status") == "READY" and gp._safe_str(plan.get("handle"))
    ]
    ready_set = set(ready)

    if only_handles:
        requested: List[str] = []
        seen = set()
        for value in only_handles:
            handle = cp._normalize_handle(value)
            if handle and handle not in seen:
                seen.add(handle)
                requested.append(handle)
        missing = [handle for handle in requested if handle not in ready_set]
        if missing:
            raise ValueError(
                "Requested handles are not READY in the current rebuilt plan: "
                f"{missing}"
            )
        selected = requested
    elif apply_all_ready:
        selected = ready
    else:
        raise ValueError(
            "No Collection selection. Pass only_handles or set apply_all_ready=True."
        )

    if max_collections is not None:
        limit = int(max_collections)
        if limit < 1:
            raise ValueError("max_collections must be >= 1 when provided.")
        selected = selected[:limit]

    if not selected:
        raise ValueError(
            "No READY Collections are available to Apply. Review Preview/BLOCKED reasons "
            "and run collection_create_prepare again after corrections."
        )

    for handle in selected:
        preview = preview_rows.get(handle)
        if not preview:
            raise ValueError(f"Preview has no row for selected handle={handle!r}.")
        if gp._safe_str(preview.get("status")).upper() != "READY":
            raise ValueError(
                f"Preview row for {handle!r} is not READY: "
                f"status={preview.get('status')!r}."
            )
    return selected


def _verify_preview_hashes(
    *,
    plans_by_handle: Mapping[str, Mapping[str, Any]],
    preview_rows: Mapping[str, Mapping[str, str]],
    selected: Sequence[str],
) -> Dict[str, Any]:
    mismatches: List[Dict[str, str]] = []
    for handle in selected:
        current_hash = gp._safe_str(plans_by_handle[handle].get("plan_hash"))
        preview_hash = gp._safe_str(preview_rows[handle].get("plan_hash"))
        if current_hash != preview_hash:
            mismatches.append(
                {
                    "handle": handle,
                    "current_plan_hash": current_hash,
                    "preview_plan_hash": preview_hash,
                }
            )
    if mismatches:
        raise RuntimeError(
            "Preview is stale or Input/Defaults/Shopify metadata changed after Prepare. "
            "Run collection_create_prepare again. "
            f"mismatches={mismatches[:10]}"
        )
    return {"status": "MATCHED", "checked": len(selected), "mismatches": []}


def _publication_ids(client: ga.ShopifyClient) -> List[str]:
    publications = ga._list_all_publications(client)
    return [gp._safe_str(item.get("id")) for item in publications if gp._safe_str(item.get("id"))]


def _publish_collection(
    *,
    client: ga.ShopifyClient,
    collection_gid: str,
    publication_ids: Sequence[str],
) -> Dict[str, Any]:
    if not publication_ids:
        raise ValueError("No Publication IDs are available.")
    data = client.gql(
        ga.M_PUBLISHABLE_PUBLISH,
        {
            "id": collection_gid,
            "input": [{"publicationId": publication_id} for publication_id in publication_ids],
        },
        operation_name="publish_collection_all_channels",
    )
    payload = data.get("publishablePublish") or {}
    user_errors = payload.get("userErrors") or []
    publishable = payload.get("publishable") or {}
    resource_count = ((publishable.get("resourcePublicationsCount") or {}).get("count"))
    available_count = ((publishable.get("availablePublicationsCount") or {}).get("count"))
    return {
        "publication_ids": list(publication_ids),
        "planned_count": len(publication_ids),
        "published_count": int(resource_count) if resource_count is not None else (0 if user_errors else len(publication_ids)),
        "available_count": int(available_count) if available_count is not None else None,
        "user_errors": user_errors,
    }


def _create_collection(client: ga.ShopifyClient, payload: Mapping[str, Any]) -> Dict[str, Any]:
    data = client.gql(
        M_COLLECTION_CREATE,
        {"collection": dict(payload)},
        operation_name="collectionCreate",
    )
    root = data.get("collectionCreate") or {}
    user_errors = root.get("userErrors") or []
    if user_errors:
        raise RuntimeError(f"collectionCreate userErrors: {user_errors}")
    collection = root.get("collection") or {}
    collection_gid = gp._safe_str(collection.get("id"))
    if not collection_gid:
        raise RuntimeError("collectionCreate returned no Collection id.")
    return dict(collection)


def _readback(client: ga.ShopifyClient, collection_gid: str) -> Dict[str, Any]:
    data = client.gql(
        Q_COLLECTION_READBACK,
        {"id": collection_gid},
        operation_name="collection_readback",
    )
    node = data.get("collection")
    if not isinstance(node, Mapping):
        raise RuntimeError(f"Collection readback returned null for {collection_gid}.")
    return dict(node)


def _readback_summary(node: Mapping[str, Any]) -> Tuple[str, int, str]:
    source_gid = ""
    condition_count = 0
    match_type = ""
    for source in node.get("sources") or []:
        if gp._safe_str(source.get("__typename")) != "CollectionConditionsSource":
            continue
        source_gid = gp._safe_str(source.get("id"))
        inclusion = source.get("inclusion") or {}
        match_type = gp._safe_str(inclusion.get("matchType"))
        condition_count += len(inclusion.get("conditions") or [])
    return source_gid, condition_count, match_type


def _result_matrix(rows: Sequence[Mapping[str, Any]]) -> List[List[Any]]:
    return [RESULT_HEADERS] + [[row.get(header, "") for header in RESULT_HEADERS] for row in rows]


def run(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_account_id: str = "Cfg__account_id",
    create_sheet_label: str = "create_collection",
    runlog_sheet_label: str = "runlog_sheet",
    tab_input: str = "Input",
    tab_defaults: str = "Defaults",
    tab_preview: str = "Preview",
    tab_result: str = "Result",
    tab_runlog: str = "Ops__RunLog",
    only_handles: Optional[Sequence[str]] = None,
    apply_all_ready: bool = True,
    max_collections: Optional[int] = None,
    dry_run: bool = True,
    confirmed: bool = False,
    stop_on_first_error: bool = False,
    write_result: bool = True,
    tz_name: str = "America/New_York",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    sa_b64_value: Optional[str] = None,
    shopify_token_value: Optional[str] = None,
    api_timeout_seconds: int = 120,
    api_max_retries: int = 6,
) -> Dict[str, Any]:
    site_code = gp._normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not dry_run and not confirmed:
        raise ValueError("Live Collection Apply requires confirmed=True.")
    if cp.MODULE_VERSION != EXPECTED_PREPARE_MODULE_VERSION:
        raise RuntimeError(
            "Prepare module version mismatch. "
            f"expected={EXPECTED_PREPARE_MODULE_VERSION}; loaded={cp.MODULE_VERSION}"
        )

    run_id = run_id or gp._make_run_id(job_name, tz_name)
    started = time.monotonic()
    phase = "apply"

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 10, f"Resolve Google access | site={site_code}")
    google_secret = gp.read_secret(
        bootstrap_gsheet_sa_b64_secret,
        project_code=site_code,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, google_auth = gp._build_gspread_client(google_secret)
    console = gp._sheets_retry("open Console Core", lambda: gc.open_by_url(console_core_url))

    progress(2, 10, "Resolve routed workbooks and account configuration")
    account = gp._load_account_values(console, tab_cfg_account_id)
    create_url = gp._resolve_sheet_url_by_label(console, tab_cfg_sites, site_code, create_sheet_label)
    runlog_url = gp._resolve_sheet_url_by_label(console, tab_cfg_sites, site_code, runlog_sheet_label)
    create_book = gp._sheets_retry("open create_collection workbook", lambda: gc.open_by_url(create_url))
    runlog_ws = gp._sheets_retry(
        f"open runlog {tab_runlog}",
        lambda: gc.open_by_url(runlog_url).worksheet(tab_runlog),
    )
    logger = gp.RunLogger18(
        worksheet=runlog_ws,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
        tz_name=tz_name,
    )

    try:
        progress(3, 10, "Initialize Shopify client and rebuild current plan")
        client = cp._shopify_client_from_account(
            account=account,
            site_code=site_code,
            shopify_token_value=shopify_token_value,
            secret_home=secret_home,
            local_secret_aliases=local_secret_aliases,
            api_timeout_seconds=api_timeout_seconds,
            api_max_retries=api_max_retries,
            print_progress=print_progress,
        )
        input_values = gp._require_worksheet(create_book, tab_input).get_all_values()
        rows = cp._read_input(input_values)
        defaults = cp._ensure_defaults(create_book, tab_defaults)
        publish_all_channels = bool(gp._normalize_bool(defaults["publish_all_channels"]))
        definitions = cp._load_metafield_definitions(client)
        grouped = cp._group_rows(rows)
        existing_by_handle: Dict[str, Optional[Mapping[str, Any]]] = {
            handle: (cp._collection_by_handle(client, handle) if handle else None)
            for handle in grouped
        }
        plans = cp._build_plans(
            rows=rows,
            metafield_definitions=definitions,
            publish_all_channels=publish_all_channels,
            existing_by_handle=existing_by_handle,
        )
        plans_by_handle = {gp._safe_str(plan.get("handle")): plan for plan in plans}

        progress(4, 10, "Read Preview and verify plan hashes")
        preview_values = gp._require_worksheet(create_book, tab_preview).get_all_values()
        preview_rows = _read_preview(preview_values)
        selected = _select_handles(
            current_plans=plans,
            preview_rows=preview_rows,
            only_handles=only_handles,
            apply_all_ready=apply_all_ready,
            max_collections=max_collections,
        )
        preview_verification = _verify_preview_hashes(
            plans_by_handle=plans_by_handle,
            preview_rows=preview_rows,
            selected=selected,
        )
        print(f"[Selection] collections={len(selected)} | handles={selected}")

        progress(5, 10, "Preload Publications when required")
        publication_ids: List[str] = []
        if any(bool(plans_by_handle[h].get("publish_all_channels")) for h in selected):
            publication_ids = _publication_ids(client)
        print(f"[Publications] available={len(publication_ids)}")

        progress(6, 10, "Re-check selected handles immediately before create")
        for handle in selected:
            existing = cp._collection_by_handle(client, handle)
            if existing:
                raise RuntimeError(
                    f"Collection handle became occupied after Prepare: {handle} | "
                    f"gid={gp._safe_str(existing.get('id'))}. Run Prepare again."
                )

        progress(7, 10, f"Apply Collections | dry_run={dry_run} | confirmed={confirmed}")
        result_rows: List[Dict[str, Any]] = []
        succeeded = 0
        failed = 0
        for index, handle in enumerate(selected, start=1):
            plan = plans_by_handle[handle]
            if print_progress:
                print(f"  [{index}/{len(selected)}] {handle}")
            base = {
                "run_id": run_id,
                "applied_at": gp._now_str(tz_name),
                "site_code": site_code,
                "dry_run": "TRUE" if dry_run else "FALSE",
                "title": plan["title"],
                "handle": handle,
                "match_type": plan["match_type"],
                "condition_count": plan["condition_count"],
                "collection_gid": "",
                "source_gid": "",
                "publish_all_channels": "TRUE" if plan["publish_all_channels"] else "FALSE",
                "publications_planned": len(publication_ids) if plan["publish_all_channels"] else 0,
                "publications_published": 0,
                "publication_ids": ",".join(publication_ids) if plan["publish_all_channels"] else "",
                "readback_condition_count": 0,
                "readback_match_type": "",
                "plan_hash": plan["plan_hash"],
                "shopify_admin_url": "",
                "storefront_url": "",
            }
            if dry_run:
                base.update(
                    {
                        "apply_status": "DRY_RUN_READY",
                        "message": "Preview hash matched; Shopify write skipped by DRY_RUN.",
                        "error_reason": "",
                    }
                )
                result_rows.append(base)
                continue

            try:
                collection = _create_collection(client, plan["payload"])
                collection_gid = gp._safe_str(collection.get("id"))
                created_handle = gp._safe_str(collection.get("handle"))
                if created_handle != handle:
                    raise RuntimeError(
                        f"collectionCreate handle mismatch: expected={handle!r}; got={created_handle!r}."
                    )

                published_count = 0
                if plan["publish_all_channels"]:
                    publication_result = _publish_collection(
                        client=client,
                        collection_gid=collection_gid,
                        publication_ids=publication_ids,
                    )
                    if publication_result["user_errors"]:
                        raise RuntimeError(
                            "Collection created but publication failed: "
                            f"{publication_result['user_errors']}"
                        )
                    published_count = int(publication_result["published_count"])

                readback = _readback(client, collection_gid)
                source_gid, readback_count, readback_match_type = _readback_summary(readback)
                if gp._safe_str(readback.get("handle")) != handle:
                    raise RuntimeError("Readback handle mismatch.")
                if readback_count != int(plan["condition_count"]):
                    raise RuntimeError(
                        "Readback condition count mismatch: "
                        f"expected={plan['condition_count']}; got={readback_count}."
                    )
                if readback_match_type != gp._safe_str(plan["match_type"]):
                    raise RuntimeError(
                        "Readback match_type mismatch: "
                        f"expected={plan['match_type']}; got={readback_match_type}."
                    )

                shop_domain = client.shop_domain
                admin_numeric = collection_gid.rsplit("/", 1)[-1]
                base.update(
                    {
                        "apply_status": "SUCCESS",
                        "collection_gid": collection_gid,
                        "source_gid": source_gid,
                        "publications_published": published_count,
                        "readback_condition_count": readback_count,
                        "readback_match_type": readback_match_type,
                        "message": "Collection created, published as configured, and readback verified.",
                        "error_reason": "",
                        "shopify_admin_url": f"https://admin.shopify.com/store/{shop_domain.split('.')[0]}/collections/{admin_numeric}",
                        "storefront_url": f"https://{shop_domain}/collections/{handle}",
                    }
                )
                succeeded += 1
            except Exception as exc:
                failed += 1
                base.update(
                    {
                        "apply_status": "FAILED",
                        "message": f"{type(exc).__name__}: {exc}",
                        "error_reason": "COLLECTION_CREATE_OR_VERIFY_FAILED",
                    }
                )
                if stop_on_first_error:
                    result_rows.append(base)
                    break
            result_rows.append(base)

        progress(8, 10, f"Overwrite Result | enabled={write_result}")
        rows_written = 0
        if write_result:
            rows_written = cp._write_single_header_matrix_overwrite(
                create_book,
                tab_result,
                _result_matrix(result_rows),
            )

        if dry_run:
            final_status = "DRY_RUN_READY" if not failed else "DRY_RUN_FAILED"
        elif failed and succeeded:
            final_status = "PARTIAL_FAILURE"
        elif failed:
            final_status = "FAILED"
        else:
            final_status = "SUCCESS"

        logger.log(
            phase=phase,
            log_type="summary",
            status=final_status,
            entity_type="COLLECTION_CREATE",
            rows_loaded=len(rows),
            rows_pending=len(selected),
            rows_recognized=len(rows),
            rows_planned=len(selected),
            rows_written=(0 if dry_run else succeeded),
            rows_skipped=failed,
            message=(
                f"dry_run={dry_run} | confirmed={confirmed} | selected={len(selected)} | "
                f"succeeded={succeeded} | failed={failed} | result_rows_written={rows_written} | "
                f"shopify_requests={client.request_count} | shopify_retries={client.retry_count}"
            ),
            error_reason="COLLECTION_CREATE_FAILURE" if failed else "",
        )
        gp._sheets_retry("write final RunLog", logger.flush)

        elapsed = round(time.monotonic() - started, 2)
        progress(9, 10, f"Finalize | status={final_status} | elapsed={elapsed}s")
        df = pd.DataFrame(result_rows, columns=RESULT_HEADERS)
        progress(10, 10, "Completed")
        return {
            "ok": final_status in {"DRY_RUN_READY", "SUCCESS"},
            "status": final_status,
            "run_id": run_id,
            "dry_run": dry_run,
            "confirmed": confirmed,
            "selected_handles": selected,
            "preview_verification": preview_verification,
            "summary": {
                "collections_selected": len(selected),
                "collections_succeeded": succeeded,
                "collections_failed": failed,
                "result_rows_written": rows_written,
                "publications_available": len(publication_ids),
                "shopify_requests": client.request_count,
                "shopify_retries": client.retry_count,
                "elapsed_seconds": elapsed,
            },
            "results": result_rows,
            "result_preview": df,
            "runtime": {
                "runtime_mode": gp._runtime_mode(),
                "google_secret_source": google_auth["source_type"],
                "shopify_api_version": client.api_version,
                "shop_domain": client.shop_domain,
                "python": sys.version.split()[0],
            },
            "targets": {
                "create_sheet_label": create_sheet_label,
                "input_tab": tab_input,
                "preview_tab": tab_preview,
                "result_tab": tab_result,
                "defaults_tab": tab_defaults,
                "runlog_sheet_label": runlog_sheet_label,
                "runlog_tab": tab_runlog,
            },
        }
    except Exception as exc:
        try:
            logger.log(
                phase=phase,
                log_type="summary",
                status="FAILED",
                entity_type="COLLECTION_CREATE",
                message=f"{type(exc).__name__}: {exc}",
                error_reason="APPLY_FAILED",
            )
            gp._sheets_retry("write failed RunLog", logger.flush)
        except Exception:
            pass
        raise


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply Shopify Collection Create plans")
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument("--bootstrap-gsheet-sa-b64-secret", required=True)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirmed", action="store_true")
    parser.add_argument("--handle", action="append", dest="handles")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result = run(
        site_code=args.site_code,
        console_core_url=args.console_core_url,
        bootstrap_gsheet_sa_b64_secret=args.bootstrap_gsheet_sa_b64_secret,
        only_handles=args.handles,
        apply_all_ready=not bool(args.handles),
        dry_run=not args.live,
        confirmed=args.confirmed,
    )
    print(json.dumps({k: v for k, v in result.items() if k not in {"results", "result_preview"}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
