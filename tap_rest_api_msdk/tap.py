"""rest-api tap class."""
import copy
import re

import requests
from pathlib import Path, PurePath
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union, cast

from genson import SchemaBuilder
from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_rest_api_msdk.streams import DynamicStream
from tap_rest_api_msdk.utils import flatten_json


class TapRestApiMsdk(Tap):
    """rest-api tap class."""
    name = "tap-rest-api-msdk"

    def __init__(self, **kwargs) -> None:
        stream_schema = th.ObjectType(
            th.Property("api_url",
                        th.StringType,
                        required=False,
                        description="the base url/endpoint for the desired api"),
            # th.Property("auth_method", th.StringType, default='no_auth', required=False),
            # th.Property("auth_token", th.StringType, required=False),
            th.Property('name', th.StringType, required=False, description="name of the stream"),
            th.Property('path',
                        th.StringType,
                        default="",
                        required=False,
                        description="the path appeneded to the `api_url`."),
            th.Property('params',
                        th.ObjectType(),
                        required=False,
                        description="an object of objects that provide the `params` in a `requests.get` method."),
            th.Property('headers',
                        th.ObjectType(),
                        required=False,
                        description="an object of headers to pass into the api calls."),
            th.Property(
                "records_path",
                th.StringType,
                default="$[*]",
                required=False,
                description="a jsonpath string representing the path in the requests response that contains the "
                "records to process. Defaults to `$[*]`."),
            th.Property("next_page_token_path",
                        th.StringType,
                        default="$.next_page",
                        required=False,
                        description="a jsonpath string representing the path to the 'next page' token. "
                        "Defaults to `$.next_page`"),
            th.Property("pagination_request_style",
                        th.StringType,
                        default="default",
                        required=False,
                        description="the pagination style to use for requests. "
                        "Defaults to `default`"),
            th.Property("pagination_response_style",
                        th.StringType,
                        default="default",
                        required=False,
                        description="the pagination style to use for response. "
                        "Defaults to `default`"),
            th.Property("pagination_page_size",
                        th.IntegerType,
                        default=None,
                        required=False,
                        description="the size of each page in records. "
                        "Defaults to None"),
            th.Property('primary_keys',
                        th.ArrayType(th.StringType),
                        required=False,
                        description="a list of the json keys of the primary key for the stream."),
            th.Property('replication_key',
                        th.StringType,
                        required=False,
                        description="the json key of the replication key. Note that this should be an incrementing "
                        "integer or datetime object."),
            th.Property(
                'except_keys',
                th.ArrayType(th.StringType),
                default=[],
                required=False,
                description="This tap automatically flattens the entire json structure and builds keys based on "
                "the corresponding paths.; Keys, whether composite or otherwise, listed in this "
                "dictionary will not be recursively flattened, but instead their values will be; "
                "turned into a json string and processed in that format. This is also automatically "
                "done for any lists within the records; therefore,; records are not duplicated for "
                "each item in lists."),
            th.Property('num_inference_records',
                        th.NumberType,
                        default=50,
                        required=False,
                        description="number of records used to infer the stream's schema. Defaults to 50."),
        )
        # Make a copy of the stream schema ObjectType as a ProperiesList (but different list)
        schema = th.PropertiesList()
        schema.wrapped = copy.copy(stream_schema.wrapped)
        # Add in headers for legacy single-stream configuration
        schema.append(
            th.Property('headers',
                        th.ObjectType(),
                        required=False,
                        description="an object of headers to pass into the api calls."), )
        # Add property for multiple streams
        schema.append(
            th.Property("streams",
                        th.ArrayType(stream_schema),
                        default=None,
                        required=False,
                        description="Array of objects that describe multiple streams"))
        # Add property for multiple streams authentication
        # This usually has a different source than rest of configuration as it contains
        # sensitive information (API credentials) and should not be checked into VCS as
        # the rest of the configuration for a data pipeline typically is.
        schema.append(
            th.Property("streams_auth",
                        th.ArrayType(th.ObjectType()),
                        default=[],
                        required=False,
                        description="List of objects with a name, match type, and auth information"))
        self.__class__.config_jsonschema = schema.to_dict()
        super().__init__(**kwargs)

    def _find_stream_auth(self, name: str) -> Dict[str, Any]:
        for auth in self.config.get('streams_auth', []):
            if auth['match_type'] == 'exact' and name == auth['name']:
                return auth
            if auth['match_type'] == 'prefix' and name.startswith(auth['prefix']):
                return auth
            if auth['match_type'] == 'suffix' and name.endswith(auth['suffix']):
                return auth
            if auth['match_type'] == 'regex' and re.match(auth['regex'], name):
                return auth
        return {}

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # Retrieve streams, or use base config as single stream if streams not present
        streams = self.config.get('streams', [self.config])
        dynamic_streams = []
        for stream in streams:
            # Do authy stuff
            auth = self._find_stream_auth(stream['name'])
            headers = stream.get('headers', {})
            headers.update(auth.get('headers', {}))
            # Maybe do other authy stuff in the future...
            dynamic_streams.append(
                DynamicStream(
                    tap=self,
                    name=stream['name'],
                    api_url=stream['api_url'],
                    path=stream['path'],
                    params=stream.get('params'),
                    headers=headers,
                    records_path=stream['records_path'],
                    next_page_token_path=stream['next_page_token_path'],
                    primary_keys=stream['primary_keys'],
                    replication_key=stream.get('replication_key'),
                    except_keys=stream.get('except_keys'),
                    schema=self.get_schema(
                        stream['api_url'],
                        stream['records_path'],
                        stream.get('except_keys'),
                        stream.get('num_inference_records'),
                        stream['path'],
                        stream.get('params'),
                        headers,
                    ),
                    pagination_request_style=stream.get('pagination_request_style', 'default'),
                    pagination_response_style=stream.get('pagination_response_style', 'default'),
                    pagination_page_size=stream.get('pagination_page_size'),
                ))
        return dynamic_streams

    def get_schema(self, api_url, records_path, except_keys, inference_records, path, params, headers) -> dict:
        """Infer schema from the first records returned by api. Creates a Stream object."""

        # todo: this request format is not very robust
        r = requests.get(api_url + path, params=params, headers=headers)
        if r.ok:
            records = extract_jsonpath(records_path, input=r.json())
        else:
            raise ValueError(r.text)

        builder = SchemaBuilder()
        builder.add_schema(th.PropertiesList().to_dict())
        for i, record in enumerate(records):
            if type(record) is not dict:
                raise ValueError("Input must be a dict object.")

            flat_record = flatten_json(record, except_keys)
            builder.add_object(flat_record)

            if i >= inference_records:
                break

        self.logger.debug(f"{builder.to_json(indent=2)}")
        return builder.to_schema()
