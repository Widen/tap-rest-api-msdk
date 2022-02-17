"""Tests stream.py features."""

import requests

from tap_rest_api_msdk.tap import TapRestApiMsdk


def config(extras: dict=None):
    contents = {
        "api_url": "http://example.com",
        "name": "stream_name",
        "auth_method": "no_auth",
        "auth_token": "",
        'path': '/path_test',
        'primary_keys': ['key1', 'key2'],
        'replication_key': 'key3',
        "records_path": "$.records[*]",
    }
    if extras:
        for k, v in extras.items():
            contents[k] = v
    return contents


def json_resp(extras: dict=None):
    contents = {
        "records": [
            {
                'key1': 'this',
                'key2': 'that',
                'key3': 'foo',
                "field1": "I",
            },
            {
                'key1': 'foo',
                'key2': 'bar',
                'key3': 'spam',
                "field2": 8
            },
        ],
    }
    if extras:
        for k, v in extras.items():
            contents[k] = v
    return contents


def url_path(path: str= '/path_test'):
    return 'http://example.com' + path


def setup_api(requests_mock, json_extras: dict=None, headers_extras: dict=None, matcher=None) -> requests.Response:
    headers_resp = {}
    if headers_extras:
        for k, v in headers_extras.items():
            headers_resp[k] = v

    adapter = requests_mock.get(url_path(), headers=headers_resp, json=json_resp(json_extras), additional_matcher=matcher)
    return requests.Session().get(url_path())


def test_get_next_page_token_default_jsonpath(requests_mock):
    resp = setup_api(requests_mock, json_extras={"next_page": "next_page_token_example"})

    stream0 = TapRestApiMsdk(config=config(), parse_env_config=True).discover_streams()[0]
    assert stream0._get_next_page_token_default(resp, "previous_token_example") == "next_page_token_example"
    assert stream0.get_next_page_token == stream0._get_next_page_token_default

def test_get_next_page_token_default_header(requests_mock):
    resp = setup_api(requests_mock, headers_extras={"X-Next-Page": "header_page_token"})

    stream0 = TapRestApiMsdk(config=config({"next_page_token_path": None}), parse_env_config=True).discover_streams()[0]
    assert stream0._get_next_page_token_default(resp, "previous_token_example") == "header_page_token"
    assert stream0.get_next_page_token == stream0._get_next_page_token_default

def test_get_next_page_token_style1_last_page(requests_mock):
    resp = setup_api(requests_mock, json_extras={"pagination": {"offset": 1, "limit": 1, "total": 2,}})
    configs = config({"pagination_response_style": "style1"})

    stream0 = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()[0]
    assert stream0._get_next_page_token_style1(resp, "previous_token_example") == 2
    assert stream0.get_next_page_token == stream0._get_next_page_token_style1

def test_get_next_page_token_style1_end(requests_mock):
    resp = setup_api(requests_mock, json_extras={"pagination": {"offset": 5, "limit": 1, "total": 2,}})
    configs = config({"pagination_response_style": "style1"})

    stream0 = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()[0]
    assert not stream0._get_next_page_token_style1(resp, "previous_token_example")
    assert stream0.get_next_page_token == stream0._get_next_page_token_style1

def test_get_url_params_default(requests_mock):
    resp = setup_api(requests_mock)

    stream0 = TapRestApiMsdk(config=config(), parse_env_config=True).discover_streams()[0]

    assert stream0._get_url_params_default({}, "next_page_token_sample") == {
        "page": "next_page_token_sample",
        "sort": "asc",
        "order_by": "key3",
    }
    assert stream0.get_url_params == stream0._get_url_params_default

def test_get_url_params_style1(requests_mock):
    resp = setup_api(requests_mock)
    configs = config({"pagination_page_size": 1, "pagination_response_style": "style1"})

    stream0 = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()[0]
    assert stream0._get_url_params_style1({}, "next_page_token_sample") == {
        "offset": "next_page_token_sample",
        "limit": 1,
        "sort": "asc",
        "order_by": "key3",
    }
    assert stream0.get_url_params == stream0._get_url_params_style1

def test_pagination_style_default(requests_mock):
    def first_matcher(request):
        return "page" not in request.url

    def second_matcher(request):
        return "page=next_page_token" in request.url

    requests_mock.get(url_path(), additional_matcher=first_matcher, json=json_resp({"next_page": "next_page_token"}))
    requests_mock.get(url_path(), additional_matcher=second_matcher, json=json_resp())

    stream0 = TapRestApiMsdk(config=config(), parse_env_config=True).discover_streams()[0]
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

def test_pagination_style_style1(requests_mock):
    def first_matcher(request):
        return "offset" not in request.url

    def second_matcher(request):
        return "offset=2" in request.url

    configs = config({"pagination_page_size": 2, "pagination_response_style": "style1"})
    json_extras = {"pagination": {"offset": 1, "limit": 1, "total": 2}}

    requests_mock.get(url_path(), additional_matcher=first_matcher, json=json_resp(json_extras))
    requests_mock.get(url_path(), additional_matcher=second_matcher, json=json_resp())

    stream0 = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()[0]
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