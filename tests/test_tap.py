from tap_rest_api_msdk.tap import TapRestApiMsdk

from tests.test_streams import config, setup_api


def test_schema_inference(requests_mock):
    setup_api(requests_mock)

    stream0 = TapRestApiMsdk(config=config(), parse_env_config=True).discover_streams()[
        0
    ]

    assert stream0.schema == {
        "$schema": "http://json-schema.org/schema#",
        "required": ["key1", "key2", "key3"],
        "type": "object",
        "properties": {
            "field1": {"type": "string"},
            "field2": {"type": "integer"},
            "key1": {"type": "string"},
            "key2": {"type": "string"},
            "key3": {"type": "string"},
        },
    }
