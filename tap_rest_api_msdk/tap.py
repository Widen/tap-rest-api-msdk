"""rest-api tap class."""

import copy
import json
from typing import Any, List, Optional

import requests
from genson import SchemaBuilder
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_rest_api_msdk.auth import ConfigurableOAuthAuthenticator, get_authenticator
from tap_rest_api_msdk.streams import DynamicStream
from tap_rest_api_msdk.utils import flatten_json


class TapRestApiMsdk(Tap):
    """rest-api tap class."""

    name = "tap-rest-api-msdk"

    # Required for Authentication in tap.py - function APIAuthenticatorBase
    tap_name = name

    # Used to cache the Authenticator to prevent over hitting the Authentication
    # end-point for each stream.
    _authenticator: Optional[APIAuthenticatorBase] = None

    common_properties = th.PropertiesList(
        th.Property(
            "path",
            th.StringType,
            required=False,
            description="the path appended to the `api_url`. Stream-level path will "
            "overwrite top-level path",
        ),
        th.Property(
            "params",
            th.ObjectType(),
            default={},
            required=False,
            description="an object providing the `params` in a `requests.get` method. "
            "Stream level params will be merged"
            "with top-level params with stream level params overwriting"
            "top-level params with the same key.",
        ),
        th.Property(
            "headers",
            th.ObjectType(),
            required=False,
            description="An object of headers to pass into the api calls. Stream level"
            "headers will be merged with top-level params with stream"
            "level params overwriting top-level params with the same key.",
        ),
        th.Property(
            "records_path",
            th.StringType,
            required=False,
            description="a jsonpath string representing the path in the requests "
            "response that contains the records to process. Defaults "
            "to `$[*]`. Stream level records_path will overwrite "
            "the top-level records_path",
        ),
        th.Property(
            "primary_keys",
            th.ArrayType(th.StringType),
            required=False,
            description="a list of the json keys of the primary key for the stream.",
        ),
        th.Property(
            "replication_key",
            th.StringType,
            required=False,
            description="the json response field representing the replication key."
            "Note that this should be an incrementing integer or datetime object.",
        ),
        th.Property(
            "except_keys",
            th.ArrayType(th.StringType),
            default=[],
            required=False,
            description="This tap automatically flattens the entire json structure "
            "and builds keys based on the corresponding paths.; Keys, "
            "whether composite or otherwise, listed in this dictionary "
            "will not be recursively flattened, but instead their values "
            "will be; turned into a json string and processed in that "
            "format. This is also automatically done for any lists within "
            "the records; therefore, records are not duplicated for each "
            "item in lists.",
        ),
        th.Property(
            "num_inference_records",
            th.NumberType,
            default=50,
            required=False,
            description="number of records used to infer the stream's schema. "
            "Defaults to 50.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="An optional field. Normally required when using the"
            "replication_key. This is the initial starting date when using a"
            "date based replication key and there is no state available.",
        ),
        th.Property(
            "source_search_field",
            th.StringType,
            required=False,
            description="An optional field name which can be used for querying "
            "specific records from supported API's. The intend for this "
            "parameter is to continue incrementally processing from a "
            "previous state. Example `last-updated`. Note: You must also "
            "set the replication_key, where the replication_key isjson "
            "response representation of the API `source_search_field`. "
            "You shouldalso supply the `source_search_query`, "
            "`replication_key` and `start_date`.",
        ),
        th.Property(
            "source_search_query",
            th.StringType,
            required=False,
            description="An optional query template to be issued against the API."
            "Substitute the query field you are querying against with "
            "$last_run_date. Atrun-time, the tap will dynamically update "
            "the token with either the `start_date`or the last bookmark / "
            "state value. A simple template Example for FHIR API's: "
            "gt$last_run_date. A more complex example against an "
            "Opensearch API, "
            '{"bool": {"filter": [{"range": '
            '{ "meta.lastUpdated": { "gt": "$last_run_date" }}}] }} .'
            "Note: Any required double quotes in the query template must "
            "be escaped.",
        ),
    )

    top_level_properties = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="the base url/endpoint for the desired api",
        ),
        th.Property(
            "auth_method",
            th.StringType,
            default="no_auth",
            required=False,
            description="The method of authentication used by the API. Supported "
            "options include oauth: for OAuth2 authentication, basic: "
            "Basic Header authorization - base64-encoded username + "
            "password config items, api_key: for API Keys in the header "
            "e.g. X-API-KEY,bearer_token: for Bearer token authorization, "
            "aws: for AWS Authentication.Defaults to no_auth which will "
            "take authentication parameters passed via the headersconfig.",
        ),
        th.Property(
            "api_keys",
            th.ObjectType(),
            required=False,
            description="A object of API Key/Value pairs used by the api_key auth "
            "method Example: { X-API-KEY: my secret value}.",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The public "
            "application ID that's assigned for Authentication. The "
            "client_id should accompany a client_secret.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The client_secret "
            "is a secret known only to the application and the "
            "authorization server. It is essential the application's "
            "own password.",
        ),
        th.Property(
            "username",
            th.StringType,
            required=False,
            description="Used for a number of authentication methods that use a user "
            "password combination for authentication.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="Used for a number of authentication methods that use a user "
            "password combination for authentication.",
        ),
        th.Property(
            "bearer_token",
            th.StringType,
            required=False,
            description="Used for the Bearer Authentication method, which uses a token "
            "as part of the authorization header for authentication.",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=False,
            description="An OAuth2 Refresh Token is a string that the OAuth2 "
            "client can use to get a new access token without the user's "
            "interaction.",
        ),
        th.Property(
            "grant_type",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The grant_type "
            "is required to describe the OAuth2 flow. Flows support by "
            "this tap include client_credentials, refresh_token, password.",
        ),
        th.Property(
            "scope",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The scope is "
            "optional, it is a mechanism to limit the amount of access "
            "that is granted to an access token. One or more scopes can "
            "be provided delimited by a space.",
        ),
        th.Property(
            "access_token_url",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. This is the "
            "end-point for the authentication server used to exchange "
            "the authorization codes for a access token.",
        ),
        th.Property(
            "redirect_uri",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. This is optional "
            "as the redirect_uri may be part of the token returned by "
            "the authentication server. If a redirect_uri is provided, "
            "it determines where the API server redirects the user after "
            "the user completes the authorization flow.",
        ),
        th.Property(
            "oauth_extras",
            th.ObjectType(),
            required=False,
            description="A object of Key/Value pairs for additional oauth config "
            "parameters which may be required by the authorization server."
            "Example: "
            "{resource: https://analysis.windows.net/powerbi/api}.",
        ),
        th.Property(
            "oauth_expiration_secs",
            th.IntegerType,
            default=None,
            required=False,
            description="Used for OAuth2 authentication method. This optional "
            "setting is a timer for the expiration of a token in "
            "seconds. If not set the OAuth will use the default "
            "expiration set in the token by the authorization server.",
        ),
        th.Property(
            "aws_credentials",
            th.ObjectType(),
            default=None,
            required=False,
            description="An object of aws credentials to authenticate to access AWS "
            "services. This example is to access the AWS OpenSearch "
            "service. Example: { aws_access_key_id: my_aws_key_id, "
            "aws_secret_access_key: my_aws_secret_access_key, "
            "aws_region: us-east-1, "
            "aws_service: es, use_signed_credentials: true} ",
        ),
        th.Property(
            "next_page_token_path",
            th.StringType,
            default=None,
            required=False,
            description="a jsonpath string representing the path to the 'next page' "
            "token. Defaults to `$.next_page`",
        ),
        th.Property(
            "pagination_request_style",
            th.StringType,
            default="default",
            required=False,
            description="the pagination style to use for requests. "
            "Defaults to `default`",
        ),
        th.Property(
            "pagination_response_style",
            th.StringType,
            default="default",
            required=False,
            description="the pagination style to use for response. "
            "Defaults to `default`",
        ),
        th.Property(
            "use_request_body_not_params",
            th.BooleanType,
            default=False,
            required=False,
            description="sends the request parameters in the request body."
            "This is normally not required, a few API's like OpenSearch"
            "require this. Defaults to `False`",
        ),
        th.Property(
            "backoff_type",
            th.StringType,
            default=None,
            required=False,
            allowed_values=[None, "message", "header"],
            description="The style of Backoff applied to rate limited APIs."
            "None: Default Meltano SDK backoff_wait_generator, message: Scans "
            "the response message for a time interval, header: retrieves the "
            "backoff value from a header key response."
            " Defaults to `None`",
        ),
        th.Property(
            "backoff_param",
            th.StringType,
            default="Retry-After",
            required=False,
            description="The name of the key which contains a the "
            "backoff value in the response. This is very applicable to backoff"
            " values in headers. Defaults to `Retry-After`",
        ),
        th.Property(
            "backoff_time_extension",
            th.IntegerType,
            default=0,
            required=False,
            description="A time extension (in seconds) to add to the backoff "
            "value from the API plus jitter. Some APIs are not precise"
            ", this adds an additional wait delay. Defaults to `0`",
        ),
        th.Property(
            "store_raw_json_message",
            th.BooleanType,
            default=False,
            required=False,
            description="Adds an additional _SDC_RAW_JSON column as an "
            "object. This will store the raw incoming message in this "
            "column when provisioned. Useful for semi-structured records "
            "when the schema is not well defined. Defaults to `False`",
        ),
        th.Property(
            "pagination_page_size",
            th.IntegerType,
            default=None,
            required=False,
            description="the size of each page in records. Defaults to None",
        ),
        th.Property(
            "pagination_results_limit",
            th.IntegerType,
            default=None,
            required=False,
            description="limits the max number of records. Defaults to None",
        ),
        th.Property(
            "pagination_next_page_param",
            th.StringType,
            default=None,
            required=False,
            description="The name of the param that indicates the page/offset. "
            "Defaults to None",
        ),
        th.Property(
            "pagination_limit_per_page_param",
            th.StringType,
            default=None,
            required=False,
            description="The name of the param that indicates the limit/per_page. "
            "Defaults to None",
        ),
        th.Property(
            "pagination_total_limit_param",
            th.StringType,
            default="total",
            required=False,
            description="The name of the param that indicates the total limit e.g. "
            "total, count. Defaults to total",
        ),
        th.Property(
            "pagination_initial_offset",
            th.IntegerType,
            default=1,
            required=False,
            description="The initial offset to start pagination from. Defaults to 1",
        ),
    )

    # add common properties to top-level properties
    for prop in common_properties.wrapped.values():
        top_level_properties.append(prop)

    # add common properties to the stream schema
    stream_properties = th.PropertiesList()
    stream_properties.wrapped = copy.copy(common_properties.wrapped)
    stream_properties.append(
        th.Property(
            "name", th.StringType, required=True, description="name of the stream"
        ),
    )
    stream_properties.append(
        th.Property(
            "schema",
            th.CustomType(
                {"anyOf": [{"type": "string"}, {"type": "null"}, {"type:": "object"}]}
            ),
            required=False,
            description="A valid Singer schema or a path-like string that provides "
            "the path to a `.json` file that contains a valid Singer "
            "schema. If provided, the schema will not be inferred from "
            "the results of an api call.",
        ),
    )

    # add streams schema to top-level properties
    top_level_properties.append(
        th.Property(
            "streams",
            th.ArrayType(th.ObjectType(*stream_properties.wrapped.values())),
            required=False,
            description="An array of streams, designed for separate paths using the"
            "same base url.",
        ),
    )

    config_jsonschema = top_level_properties.to_dict()

    def discover_streams(self) -> List[DynamicStream]:  # type: ignore
        """Return a list of discovered streams.

        Returns:
            A list of streams.

        """
        # print(self.top_level_properties.to_dict())

        streams = []
        for stream in self.config["streams"]:
            # resolve config
            records_path = stream.get(
                "records_path", self.config.get("records_path", "$[*]")
            )
            except_keys = stream.get("except_keys", self.config.get("except_keys", []))
            path = stream.get("path", self.config.get("path", ""))
            params = {**self.config.get("params", {}), **stream.get("params", {})}
            headers = {**self.config.get("headers", {}), **stream.get("headers", {})}
            start_date = stream.get("start_date", self.config.get("start_date", ""))
            replication_key = stream.get(
                "replication_key", self.config.get("replication_key", "")
            )
            source_search_field = stream.get(
                "source_search_field", self.config.get("source_search_field", "")
            )
            source_search_query = stream.get(
                "source_search_query", self.config.get("source_search_query", "")
            )

            schema = {}
            schema_config = stream.get("schema")
            if isinstance(schema_config, str):
                self.logger.info("Found path to a schema, not doing discovery.")
                with open(schema_config, "r") as f:
                    schema = json.load(f)

            elif isinstance(schema_config, dict):
                self.logger.info("Found schema in config, not doing discovery.")
                builder = SchemaBuilder()
                builder.add_schema(schema_config)
                schema = builder.to_schema()

            else:
                self.logger.info("No schema found. Inferring schema from API call.")
                schema = self.get_schema(
                    records_path,
                    except_keys,
                    stream.get(
                        "num_inference_records",
                        self.config["num_inference_records"],
                    ),
                    path,
                    params,
                    headers,
                )

            streams.append(
                DynamicStream(
                    tap=self,
                    name=stream["name"],
                    path=path,
                    params=params,
                    headers=headers,
                    records_path=records_path,
                    primary_keys=stream.get(
                        "primary_keys", self.config.get("primary_keys", [])
                    ),
                    replication_key=replication_key,
                    except_keys=except_keys,
                    next_page_token_path=self.config.get("next_page_token_path"),
                    pagination_request_style=self.config["pagination_request_style"],
                    pagination_response_style=self.config["pagination_response_style"],
                    pagination_page_size=self.config.get("pagination_page_size"),
                    pagination_results_limit=self.config.get(
                        "pagination_results_limit"
                    ),
                    pagination_next_page_param=self.config.get(
                        "pagination_next_page_param"
                    ),
                    pagination_limit_per_page_param=self.config.get(
                        "pagination_limit_per_page_param"
                    ),
                    pagination_total_limit_param=self.config.get(
                        "pagination_total_limit_param"
                    ),
                    pagination_initial_offset=self.config.get(
                        "pagination_initial_offset",
                        1,
                    ),
                    schema=schema,
                    start_date=start_date,
                    source_search_field=source_search_field,
                    source_search_query=source_search_query,
                    use_request_body_not_params=self.config.get(
                        "use_request_body_not_params"
                    ),
                    backoff_type=self.config.get("backoff_type"),
                    backoff_param=self.config.get("backoff_param"),
                    backoff_time_extension=self.config.get("backoff_time_extension"),
                    store_raw_json_message=self.config.get("store_raw_json_message"),
                    authenticator=self._authenticator,
                )
            )

        return streams

    def get_schema(
        self,
        records_path: str,
        except_keys: list,
        inference_records: int,
        path: str,
        params: dict,
        headers: dict,
    ) -> Any:
        """Infer schema from the first records returned by api. Creates a Stream object.

        If auth_method is set, will call get_authenticator to obtain credentials
        to issue a request to sample some records. The get_authenticator will:
        - stores the authenticator in self._authenticator
        - sets the self.http_auth if required by a given authenticator
        - use an existing authenticator if one exists and is cached.

        Args:
            records_path: required - see config_jsonschema.
            except_keys: required - see config_jsonschema.
            inference_records: required - see config_jsonschema.
            path: required - see config_jsonschema.
            params: required - see config_jsonschema.
            headers: required - see config_jsonschema.

        Raises:
            ValueError: if the response is not valid or a record is not valid json.

        Returns:
            A schema for the stream.

        """
        # TODO: this request format is not very robust

        # Initialise Variables
        auth_method = self.config.get("auth_method", "")
        self.http_auth = None

        if auth_method and not auth_method == "no_auth":
            # Obtaining Authenticator for authorisation to obtain a schema.
            get_authenticator(self)

            # Get an initial oauth token if an oauth method
            if auth_method == "oauth" and isinstance(
                self._authenticator, ConfigurableOAuthAuthenticator
            ):
                self._authenticator.get_initial_oauth_token()

            headers.update(getattr(self._authenticator, "auth_headers", {}))
            params.update(getattr(self._authenticator, "auth_params", {}))

        r = requests.get(
            self.config["api_url"] + path,
            auth=self.http_auth,
            params=params,
            headers=headers,
        )
        if r.ok:
            records = extract_jsonpath(records_path, input=r.json())
        else:
            self.logger.error(f"Error Connecting, message = {r.text}")
            raise ValueError(r.text)

        builder = SchemaBuilder()
        builder.add_schema(th.PropertiesList().to_dict())
        for i, record in enumerate(records):
            if type(record) is not dict:
                self.logger.error("Input must be a dict object.")
                raise ValueError("Input must be a dict object.")

            flat_record = flatten_json(
                record, except_keys, store_raw_json_message=False
            )

            builder.add_object(flat_record)
            # Optional add _sdc_raw_json field to store the raw message
            if self.config.get("store_raw_json_message"):
                builder.add_object({"_sdc_raw_json": {}})

            if i >= inference_records:
                break

        self.logger.debug(f"{builder.to_json(indent=2)}")
        return builder.to_schema()
