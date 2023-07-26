"""Tests stream.py features."""

from typing import Any

import requests
from tap_rest_api_msdk.tap import TapRestApiMsdk


def config(extras: dict = None) -> dict:
    """Utility function giving a basic/common config for streams.

    Args:
        extras: items to add to the basic config.

    Returns:
        A complete tap config.
    """
    contents = {
        "api_url": "https://example.com",
        "streams": [
            {
                "name": "stream_name",
                "path": "/path_test",
                "primary_keys": ["key1", "key2"],
                "replication_key": "key3",
                "records_path": "$.records[*]",
            }
        ],
    }
    if extras:
        for k, v in extras.items():
            contents[k] = v
    return contents


def json_resp(extras: dict = None) -> dict:
    """Utility function returning a common response for mocked API calls.

    Args:
        extras: items to be added to the contents of the json response.

    Returns:
        A json object that mocks the results of an API call.
    """
    contents = {
        "records": [
            {
                "key1": "this",
                "key2": "that",
                "key3": "foo",
                "field1": "I",
            },
            {"key1": "foo", "key2": "bar", "key3": "spam", "field2": 8},
        ],
    }
    if extras:
        for k, v in extras.items():
            contents[k] = v
    return contents


def url_path(path: str = "/path_test") -> str:
    """Utility function returning a common url for mocked API calls.

    Args:
        path: a path to add to the end of the base url.

    Returns:
        A full url.
    """
    return "https://example.com" + path


def setup_api(
    requests_mock: Any,
    url_path: str = url_path(),
    json_extras: dict = None,
    headers_extras: dict = None,
    matcher: Any = None,
) -> requests.Response:
    """Utility function for mocking API calls.

    Args:
        requests_mock: mock object for requests.
        url_path: url to mack for mocking.
        json_extras: extra items to add to the response's results.
        headers_extras: extra items to add to the API call's header.
        matcher: a function that checks a request's input for the appropriate
            configuration.

    Returns:
        A mocked requests.Response object.
    """
    headers_resp = {}
    if headers_extras:
        for k, v in headers_extras.items():
            headers_resp[k] = v

    requests_mock.get(
        url_path,
        headers=headers_resp,
        json=json_resp(json_extras),
        additional_matcher=matcher,
    )
    return requests.Session().get(url_path)


def test_pagination_style_default(requests_mock):
    def first_matcher(request):
        return "page" not in request.url

    def second_matcher(request):
        return "page=next_page_token" in request.url

    requests_mock.get(
        url_path(),
        additional_matcher=first_matcher,
        json=json_resp({"next_page": "next_page_token"}),
    )
    requests_mock.get(url_path(), additional_matcher=second_matcher, json=json_resp())

    stream0 = TapRestApiMsdk(config=config(), parse_env_config=True).discover_streams()[
        0
    ]
    records_gen = stream0.get_records({})
    records = []
    for record in records_gen:
        records.append(record)

    assert records == [
        json_resp()["records"][0],
        json_resp()["records"][1],
        json_resp()["records"][0],
        json_resp()["records"][1],
    ]
