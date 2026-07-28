import sys
import types
import unittest
from unittest import mock

from shopify_setup import sync_locations as locations


class _Worksheet:
    def __init__(self, values):
        self._values = values

    def get_all_values(self):
        return self._values


class _Workbook:
    title = "Workspace Registry"

    def __init__(self, values):
        self._worksheet = _Worksheet(values)

    def worksheet(self, title):
        self.requested_tab = title
        return self._worksheet


class _GspreadClient:
    def __init__(self, values):
        self._workbook = _Workbook(values)

    def open_by_key(self, file_id):
        self.requested_file_id = file_id
        return self._workbook


class SyncLocationsTests(unittest.TestCase):
    def test_workspace_registry_routes_active_project(self):
        values = [
            [
                "project_code",
                "project_name",
                "active",
                "console_core_url",
                "gsheet_secret_name",
                "account_config_tab",
                "timezone",
                "notes",
            ],
            [
                "APOLLO",
                "Apollo",
                "TRUE",
                "https://docs.google.com/spreadsheets/d/project-sheet/edit",
                "Apollo_GSHEET",
                "Cfg__account_id",
                "America/Chicago",
                "Material handling",
            ],
        ]
        client = _GspreadClient(values)

        with (
            mock.patch.object(
                locations,
                "read_secret",
                return_value=locations.SecretValue(
                    "not-a-real-secret",
                    "TEST",
                    "test",
                ),
            ) as read_secret,
            mock.patch.object(
                locations,
                "_build_gspread_client",
                return_value=(
                    client,
                    {
                        "source_type": "TEST",
                        "service_account_email": "test@example.invalid",
                    },
                ),
            ),
        ):
            route = locations.resolve_workspace_project(
                project_code="apollo",
                workspace_registry_id=(
                    "https://docs.google.com/spreadsheets/d/"
                    "workspace-registry/edit"
                ),
                print_progress=False,
            )

        read_secret.assert_called_once_with(
            "WORKSPACE_GSHEET",
            project_code="WORKSPACE",
            explicit_value=None,
            secret_home=None,
        )
        self.assertEqual(client.requested_file_id, "workspace-registry")
        self.assertEqual(route["project_code"], "APOLLO")
        self.assertEqual(route["gsheet_secret_name"], "Apollo_GSHEET")
        self.assertEqual(route["account_config_tab"], "Cfg__account_id")
        self.assertEqual(route["timezone"], "America/Chicago")

    def test_local_secret_uses_canonical_project_aliases(self):
        calls = {}

        class FakeResolver:
            def __init__(self, project_code, secret_home=None):
                calls["project_code"] = project_code
                calls["secret_home"] = secret_home

            def read(self, name, aliases=()):
                calls["name"] = name
                calls["aliases"] = aliases
                return types.SimpleNamespace(
                    value="not-a-real-secret",
                    source_type="TEST",
                    resolved_name=aliases[0],
                    path=None,
                    key=None,
                )

        fake_module = types.SimpleNamespace(
            WorkspaceSecretResolver=FakeResolver
        )
        with (
            mock.patch.object(
                locations,
                "_runtime_mode",
                return_value="LOCAL",
            ),
            mock.patch.dict(
                sys.modules,
                {"workspace_secret_resolver": fake_module},
            ),
        ):
            result = locations.read_secret(
                "Apollo_SHOPIFY_ACCESS_TOKEN",
                project_code="apollo",
            )

        self.assertEqual(calls["project_code"], "APOLLO")
        self.assertEqual(calls["name"], "Apollo_SHOPIFY_ACCESS_TOKEN")
        self.assertEqual(
            calls["aliases"],
            ("APOLLO_SHOPIFY_ACCESS_TOKEN",),
        )
        self.assertEqual(result.source_detail, "APOLLO_SHOPIFY_ACCESS_TOKEN")

    def test_sync_plan_updates_system_fields_and_preserves_human_fields(self):
        existing = [
            {
                "site_code": "APOLLO",
                "location_code": "IL-3",
                "location_name": "Old name",
                "location_gid": "gid://shopify/Location/1",
                "province_code": "XX",
                "active": "FALSE",
                "is_default": "TRUE",
                "notes": "Keep this operator note",
                "synced_at": "old timestamp",
            }
        ]
        nodes = [
            {
                "id": "gid://shopify/Location/1",
                "name": "IL60487 Warehouse",
                "isActive": True,
                "address": {"provinceCode": "IL"},
            }
        ]

        plan = locations._build_sync_plan(
            site_code="APOLLO",
            existing_headers=locations.LOCATION_HEADERS,
            existing_records=existing,
            shopify_nodes=nodes,
            synced_at="2026-07-29 10:00:00",
        )
        record = plan["records"][0]

        self.assertEqual(record["location_code"], "IL-3")
        self.assertEqual(record["is_default"], "TRUE")
        self.assertEqual(record["notes"], "Keep this operator note")
        self.assertEqual(record["location_name"], "IL60487 Warehouse")
        self.assertEqual(record["province_code"], "IL")
        self.assertEqual(record["active"], "TRUE")
        self.assertEqual(record["synced_at"], "2026-07-29 10:00:00")
        self.assertEqual(plan["stats"]["updated"], 1)


if __name__ == "__main__":
    unittest.main()
