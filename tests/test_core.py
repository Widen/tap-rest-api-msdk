"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_standard_tap_tests
from tap_rest_api_msdk.tap import TapRestApiMsdk

from tests.test_streams import config, json_resp, url_path


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(requests_mock):
    """Run standard tap tests from the SDK."""
    requests_mock.get(url_path(), json=json_resp())
    tests = get_standard_tap_tests(TapRestApiMsdk, config=config())
    for test in tests:
        test()
