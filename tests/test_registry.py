import inspect
import unittest

import console_core
from console_core import resolve_sheet_resource


SHEET_ID = "14Hcig_nxulNVO8cneMEol0XbJq-I0Jw_oXRdQDDc61g"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"


def project_rows(**overrides):
    row = {
        "project_code": "PBS",
        "active": "TRUE",
        "console_core_url": "https://docs.google.com/spreadsheets/d/console/edit",
    }
    row.update(overrides)
    return [row]


def site_rows(**overrides):
    row = {
        "site_code": "PBS",
        "label": "sh_competitor_crawler",
        "sheet_url": SHEET_URL,
        "active": "TRUE",
    }
    row.update(overrides)
    return [row]


def resolve(project=None, sites=None, **kwargs):
    return resolve_sheet_resource(
        project_code=kwargs.get("project_code", "PBS"),
        site_code=kwargs.get("site_code", "PBS"),
        sheet_label=kwargs.get("sheet_label", "sh_competitor_crawler"),
        project_registry_rows=project if project is not None else project_rows(),
        cfg_sites_rows=sites if sites is not None else site_rows(),
    )


class RegistryTests(unittest.TestCase):
    def test_public_export_and_signature(self):
        self.assertEqual(console_core.__all__, ["resolve_sheet_resource"])
        self.assertEqual(
            list(inspect.signature(resolve_sheet_resource).parameters),
            [
                "project_code",
                "site_code",
                "sheet_label",
                "project_registry_rows",
                "cfg_sites_rows",
            ],
        )

    def test_valid_pbs_fixture_and_safe_provenance(self):
        route = resolve()
        self.assertEqual(route["project_code"], "PBS")
        self.assertEqual(route["site_code"], "PBS")
        self.assertEqual(route["sheet_label"], "sh_competitor_crawler")
        self.assertEqual(route["sheet_id"], SHEET_ID)
        self.assertEqual(route["sheet_url"], SHEET_URL)
        self.assertEqual(route["active"], "TRUE")
        self.assertEqual(route["project_registry_source_row"], "2")
        self.assertEqual(route["cfg_sites_source_row"], "2")
        self.assertEqual(route["project_registry_tab"], "Cfg__Projects")
        self.assertEqual(route["cfg_sites_tab"], "Cfg__Sites")
        self.assertNotIn("execution_project_code", route)
        self.assertFalse(any("secret" in key.lower() for key in route))

    def test_execution_identity_is_irrelevant(self):
        route = resolve()
        self.assertEqual(route["project_code"], "PBS")

    def test_required_identities(self):
        for key in ("project_code", "site_code", "sheet_label"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                resolve(**{key: " "})

    def test_missing_duplicate_and_inactive_project(self):
        with self.assertRaises(ValueError):
            resolve(project=[])
        with self.assertRaises(ValueError):
            resolve(project=project_rows() + project_rows())
        with self.assertRaises(ValueError):
            resolve(project=project_rows(active="FALSE"))

    def test_project_headers_and_empty_route(self):
        with self.assertRaises(ValueError):
            resolve(project=[{"project_code": "PBS", "active": "TRUE"}])
        with self.assertRaises(ValueError):
            resolve(
                project=[
                    {
                        "project_code": "PBS",
                        "project code": "PBS",
                        "active": "TRUE",
                        "console_core_url": "url",
                    }
                ]
            )
        with self.assertRaises(ValueError):
            resolve(project=project_rows(console_core_url=""))

    def test_missing_duplicate_and_inactive_resource(self):
        with self.assertRaises(ValueError):
            resolve(sites=[])
        with self.assertRaises(ValueError):
            resolve(sites=site_rows() + site_rows())
        with self.assertRaises(ValueError):
            resolve(sites=site_rows(active="FALSE"))

    def test_resource_without_active_column_preserves_existing_schema(self):
        rows = site_rows()[0]
        rows.pop("active")
        route = resolve(sites=[rows])
        self.assertEqual(route["active"], "")

    def test_resource_headers_and_empty_identity(self):
        with self.assertRaises(ValueError):
            resolve(sites=[{"site_code": "PBS", "sheet_url": SHEET_URL}])
        with self.assertRaises(ValueError):
            resolve(sites=site_rows(sheet_url="", sheet_id=""))

    def test_url_only_id_only_and_consistent_url_id(self):
        self.assertEqual(resolve()["sheet_id"], SHEET_ID)
        id_only = site_rows(sheet_url="", sheet_id=SHEET_ID)
        self.assertEqual(resolve(sites=id_only)["sheet_url"], "")
        both = site_rows(sheet_id=SHEET_ID)
        self.assertEqual(resolve(sites=both)["sheet_id"], SHEET_ID)

    def test_conflicting_id_and_malformed_url(self):
        with self.assertRaises(ValueError):
            resolve(sites=site_rows(sheet_id="different"))
        with self.assertRaises(ValueError):
            resolve(sites=site_rows(sheet_url="not-a-sheet-url"))

    def test_label_matching_is_normalized(self):
        route = resolve(sheet_label=" SH_COMPETITOR_CRAWLER ")
        self.assertEqual(route["sheet_label"], "SH_COMPETITOR_CRAWLER")


if __name__ == "__main__":
    unittest.main()
