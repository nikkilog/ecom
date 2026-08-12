import inspect
import unittest

import console_core
from console_core import load_registry_rows, resolve_sheet_resource
from console_core import registry_io


WORKSPACE_REGISTRY_ID = "1-5dXTm3ZWHTVCoXB9nG7A4CtqsCZPmw-UQckWotMjwQ"
WORKSPACE_REGISTRY_URL = (
    "https://docs.google.com/spreadsheets/d/"
    f"{WORKSPACE_REGISTRY_ID}/edit"
)
CONSOLE_ID = "1PHHwMAGQrkQ7V_Y7WziJodlkj4UBPrO3W8vSRY6D7oY"
CONSOLE_URL = f"https://docs.google.com/spreadsheets/d/{CONSOLE_ID}/edit"
TARGET_ID = "14Hcig_nxulNVO8cneMEol0XbJq-I0Jw_oXRdQDDc61g"
TARGET_URL = f"https://docs.google.com/spreadsheets/d/{TARGET_ID}/edit"

PROJECT_VALUES = [
    ["project_code", "active", "console_core_url"],
    ["PBS", "TRUE", CONSOLE_URL],
]
CFG_SITES_VALUES = [
    ["site_code", "label", "sheet_url", "active"],
    ["PBS", "sh_competitor_crawler", TARGET_URL, "TRUE"],
]


class FakeResponse:
    def __init__(self, status, retry_after=None):
        self.status_code = status
        self.headers = {}
        if retry_after is not None:
            self.headers["Retry-After"] = str(retry_after)


class FakeGoogleError(Exception):
    def __init__(self, status, retry_after=None):
        super().__init__(f"HTTP {status}")
        self.response = FakeResponse(status, retry_after)


class FakeWorksheet:
    def __init__(self, values, failures=None):
        self.values = values
        self.failures = list(failures or [])
        self.read_count = 0

    def get_all_values(self):
        self.read_count += 1
        if self.failures:
            raise self.failures.pop(0)
        return self.values


class FakeWorkbook:
    def __init__(self, worksheets):
        self.worksheets = worksheets
        self.requested_tabs = []

    def worksheet(self, title):
        self.requested_tabs.append(title)
        value = self.worksheets[title]
        if isinstance(value, BaseException):
            raise value
        return value


class FakeGoogleClient:
    def __init__(self, registry_book, console_book):
        self.registry_book = registry_book
        self.console_book = console_book
        self.opened_keys = []
        self.opened_urls = []

    def open_by_key(self, key):
        self.opened_keys.append(key)
        return self.registry_book

    def open_by_url(self, url):
        self.opened_urls.append(url)
        return self.console_book


def fake_client(project_values=None, cfg_sites_values=None):
    registry_ws = FakeWorksheet(
        PROJECT_VALUES if project_values is None else project_values
    )
    sites_ws = FakeWorksheet(
        CFG_SITES_VALUES if cfg_sites_values is None else cfg_sites_values
    )
    return FakeGoogleClient(
        FakeWorkbook({"Cfg__Projects": registry_ws}),
        FakeWorkbook({"Cfg__Sites": sites_ws}),
    )


class RegistryIOTests(unittest.TestCase):
    def test_public_exports_and_signature(self):
        self.assertEqual(
            console_core.__all__,
            ["load_registry_rows", "resolve_sheet_resource"],
        )
        self.assertEqual(
            list(inspect.signature(load_registry_rows).parameters),
            [
                "google_client",
                "workspace_registry_id",
                "project_code",
                "workspace_registry_tab",
                "cfg_sites_tab",
                "print_progress",
            ],
        )

    def test_registry_open_by_id_and_normal_url(self):
        for location in (WORKSPACE_REGISTRY_ID, WORKSPACE_REGISTRY_URL):
            with self.subTest(location=location):
                client = fake_client()
                result = load_registry_rows(
                    google_client=client,
                    workspace_registry_id=location,
                    project_code="pbs",
                    print_progress=False,
                )
                self.assertEqual(client.opened_keys, [WORKSPACE_REGISTRY_ID])
                self.assertEqual(
                    result["workspace_registry_id"], WORKSPACE_REGISTRY_ID
                )

    def test_registry_location_validation(self):
        for location in ("", " ", "https://example.com/not-a-sheet"):
            with self.subTest(location=location), self.assertRaises(ValueError):
                load_registry_rows(
                    google_client=fake_client(),
                    workspace_registry_id=location,
                    project_code="PBS",
                    print_progress=False,
                )

    def test_registry_and_cfg_sites_worksheet_lookup(self):
        client = fake_client()
        load_registry_rows(
            google_client=client,
            workspace_registry_id=WORKSPACE_REGISTRY_ID,
            project_code="PBS",
            print_progress=False,
        )
        self.assertEqual(
            client.registry_book.requested_tabs, ["Cfg__Projects"]
        )
        self.assertEqual(client.console_book.requested_tabs, ["Cfg__Sites"])

    def test_missing_worksheets_fail_immediately(self):
        for missing_registry in (True, False):
            with self.subTest(missing_registry=missing_registry):
                missing = KeyError("missing worksheet")
                registry_book = FakeWorkbook(
                    {
                        "Cfg__Projects": (
                            missing
                            if missing_registry
                            else FakeWorksheet(PROJECT_VALUES)
                        )
                    }
                )
                console_book = FakeWorkbook(
                    {
                        "Cfg__Sites": (
                            missing
                            if not missing_registry
                            else FakeWorksheet(CFG_SITES_VALUES)
                        )
                    }
                )
                with self.assertRaises(KeyError):
                    load_registry_rows(
                        google_client=FakeGoogleClient(registry_book, console_book),
                        workspace_registry_id=WORKSPACE_REGISTRY_ID,
                        project_code="PBS",
                        print_progress=False,
                    )

    def test_empty_and_header_only_worksheets(self):
        for project_values, site_values in (
            ([], CFG_SITES_VALUES),
            ([PROJECT_VALUES[0]], CFG_SITES_VALUES),
            (PROJECT_VALUES, []),
            (PROJECT_VALUES, [CFG_SITES_VALUES[0]]),
        ):
            with self.subTest(
                project_values=project_values,
                site_values=site_values,
            ):
                with self.assertRaises(ValueError):
                    load_registry_rows(
                        google_client=fake_client(project_values, site_values),
                        workspace_registry_id=WORKSPACE_REGISTRY_ID,
                        project_code="PBS",
                        print_progress=False,
                    )

    def test_values_convert_to_safe_mapping_rows(self):
        result = load_registry_rows(
            google_client=fake_client(),
            workspace_registry_id=WORKSPACE_REGISTRY_ID,
            project_code="PBS",
            print_progress=False,
        )
        self.assertEqual(
            result["project_registry_rows"][0]["project_code"], "PBS"
        )
        self.assertEqual(
            result["cfg_sites_rows"][0]["label"],
            "sh_competitor_crawler",
        )

    def test_duplicate_normalized_headers_fail_before_mapping_loss(self):
        values = [
            ["project_code", "project code", "active", "console_core_url"],
            ["PBS", "PBS", "TRUE", CONSOLE_URL],
        ]
        with self.assertRaisesRegex(ValueError, "duplicate normalized headers"):
            load_registry_rows(
                google_client=fake_client(project_values=values),
                workspace_registry_id=WORKSPACE_REGISTRY_ID,
                project_code="PBS",
                print_progress=False,
            )

    def test_shared_project_selector_opens_selected_console_url(self):
        client = fake_client()
        result = load_registry_rows(
            google_client=client,
            workspace_registry_id=WORKSPACE_REGISTRY_ID,
            project_code="pbs",
            print_progress=False,
        )
        self.assertEqual(client.opened_urls, [CONSOLE_URL])
        self.assertEqual(result["console_core_url"], CONSOLE_URL)
        with self.assertRaises(ValueError):
            load_registry_rows(
                google_client=fake_client(),
                workspace_registry_id=WORKSPACE_REGISTRY_ID,
                project_code="MISSING",
                print_progress=False,
            )

    def test_safe_provenance_contains_no_auth_material(self):
        result = load_registry_rows(
            google_client=fake_client(),
            workspace_registry_id=WORKSPACE_REGISTRY_ID,
            project_code="PBS",
            print_progress=False,
        )
        self.assertEqual(result["project_code"], "PBS")
        self.assertEqual(result["workspace_registry_tab"], "Cfg__Projects")
        self.assertEqual(result["cfg_sites_tab"], "Cfg__Sites")
        self.assertFalse(
            any(
                token in key.lower()
                for key in result
                for token in ("secret", "credential", "token", "private_key")
            )
        )

    def test_retryable_http_statuses(self):
        for status in (429, 500, 502, 503, 504):
            attempts = []

            def operation():
                attempts.append(status)
                if len(attempts) == 1:
                    raise FakeGoogleError(status)
                return "ok"

            sleeps = []
            result = registry_io._with_sheets_retry(
                operation,
                action="test",
                print_progress=False,
                sleep=sleeps.append,
                random_value=lambda: 0.0,
            )
            self.assertEqual(result, "ok")
            self.assertEqual(len(attempts), 2)
            self.assertEqual(sleeps, [1.5])

    def test_retry_after_is_honored_and_bounded(self):
        for retry_after, expected in ((7, 7.0), (999, 20.0)):
            attempts = []

            def operation():
                attempts.append(1)
                if len(attempts) == 1:
                    raise FakeGoogleError(429, retry_after=retry_after)
                return "ok"

            sleeps = []
            registry_io._with_sheets_retry(
                operation,
                action="test",
                print_progress=False,
                sleep=sleeps.append,
                random_value=lambda: 0.0,
            )
            self.assertEqual(sleeps, [expected])

    def test_transport_failures_retry(self):
        transport_errors = (
            TimeoutError("timed out"),
            ConnectionResetError("reset"),
            ConnectionAbortedError("aborted"),
            BrokenPipeError("broken pipe"),
        )
        for error in transport_errors:
            attempts = []

            def operation():
                attempts.append(1)
                if len(attempts) == 1:
                    raise error
                return "ok"

            self.assertEqual(
                registry_io._with_sheets_retry(
                    operation,
                    action="test",
                    print_progress=False,
                    sleep=lambda _delay: None,
                    random_value=lambda: 0.0,
                ),
                "ok",
            )
            self.assertEqual(len(attempts), 2)

    def test_named_runtime_transport_failure_retries(self):
        RuntimeTimeout = type("ReadTimeout", (Exception,), {})
        attempts = []

        def operation():
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeTimeout("runtime timeout")
            return "ok"

        self.assertEqual(
            registry_io._with_sheets_retry(
                operation,
                action="test",
                print_progress=False,
                sleep=lambda _delay: None,
                random_value=lambda: 0.0,
            ),
            "ok",
        )

    def test_nonretryable_error_raises_immediately(self):
        attempts = []

        def operation():
            attempts.append(1)
            raise ValueError("invalid request")

        with self.assertRaisesRegex(ValueError, "invalid request"):
            registry_io._with_sheets_retry(
                operation,
                action="test",
                print_progress=False,
                sleep=lambda _delay: self.fail("must not sleep"),
            )
        self.assertEqual(len(attempts), 1)

    def test_maximum_attempts_are_bounded(self):
        attempts = []
        sleeps = []

        def operation():
            attempts.append(1)
            raise FakeGoogleError(503)

        with self.assertRaises(FakeGoogleError):
            registry_io._with_sheets_retry(
                operation,
                action="test",
                print_progress=False,
                sleep=sleeps.append,
                random_value=lambda: 0.0,
            )
        self.assertEqual(len(attempts), registry_io._MAX_ATTEMPTS)
        self.assertEqual(len(sleeps), registry_io._MAX_ATTEMPTS - 1)
        self.assertTrue(all(delay <= 20.0 for delay in sleeps))

    def test_no_auth_or_secret_dependency(self):
        source = inspect.getsource(registry_io)
        for forbidden in (
            "WorkspaceSecretResolver",
            "workspace_secret_resolver",
            "read_secret(",
            "service_account",
            "gspread.authorize",
        ):
            self.assertNotIn(forbidden, source)

    def test_complete_fake_pbs_flow_is_deterministic(self):
        for _ in range(2):
            rows = load_registry_rows(
                google_client=fake_client(),
                workspace_registry_id=WORKSPACE_REGISTRY_ID,
                project_code="PBS",
                print_progress=False,
            )
            resource = resolve_sheet_resource(
                project_code="PBS",
                site_code="PBS",
                sheet_label="sh_competitor_crawler",
                project_registry_rows=rows["project_registry_rows"],
                cfg_sites_rows=rows["cfg_sites_rows"],
            )
            self.assertEqual(resource["sheet_id"], TARGET_ID)


if __name__ == "__main__":
    unittest.main()
