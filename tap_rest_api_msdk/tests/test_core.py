"""Tests standard tap features using the built-in SDK tests library."""

import pytest
import requests

from tap_rest_api_msdk.tap import TapRestApiMsdk
from tap_rest_api_msdk.tests.test_streams import url_path, json_resp, config

from singer_sdk.testing import get_standard_tap_tests


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(requests_mock):
    """Run standard tap tests from the SDK."""
    requests_mock.get(url_path(), json=json_resp())
    tests = get_standard_tap_tests(TapRestApiMsdk, config=config())
    for test in tests:
        test()

