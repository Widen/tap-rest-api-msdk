import json

from tap_rest_api_msdk.tap import TapRestApiMsdk

from tests.test_streams import config, setup_api

with open("tests/schema.json", "r") as f:
    BASIC_SCHEMA = json.load(f)


def test_schema_inference(requests_mock):
    setup_api(requests_mock)

    stream0 = TapRestApiMsdk(config=config(), parse_env_config=True).discover_streams()[
        0
    ]

    assert stream0.schema == BASIC_SCHEMA


def test_schema_from_file():
    configs = config()
    configs["streams"][0]["schema"] = "tests/schema.json"

    s0 = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()[0]

    assert s0.schema == BASIC_SCHEMA


def test_schema_from_object():
    configs = config()
    configs["streams"][0]["schema"] = BASIC_SCHEMA

    s0 = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()[0]

    assert s0.schema == BASIC_SCHEMA


def test_multiple_streams(requests_mock):
    setup_api(requests_mock)
    setup_api(requests_mock, url_path="https://example.com/path_test2")
    configs = config({"records_path": "$.records[*]"})
    configs["streams"].append(
        {
            "name": "stream_name2",
            "path": "/path_test2",
            "primary_keys": ["key4", "key5"],
            "replication_key": "key6",
        }
    )

    streams = TapRestApiMsdk(config=configs, parse_env_config=True).discover_streams()

    assert streams[0].name == "stream_name"
    assert streams[0].records_path == "$.records[*]"
    assert streams[0].path == "/path_test"
    assert streams[0].primary_keys == ["key1", "key2"]
    assert streams[0].replication_key == "key3"
    assert streams[1].name == "stream_name2"
    assert streams[1].records_path == "$.records[*]"
    assert streams[1].path == "/path_test2"
    assert streams[1].primary_keys == ["key4", "key5"]
    assert streams[1].replication_key == "key6"
