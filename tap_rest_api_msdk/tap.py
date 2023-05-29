"""rest-api tap class."""

import copy
import json
from typing import Any, List

import requests
from genson import SchemaBuilder
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.authenticators import APIAuthenticatorBase, APIKeyAuthenticator, BasicAuthenticator, BearerTokenAuthenticator, OAuthAuthenticator
from tap_rest_api_msdk.streams import DynamicStream
from tap_rest_api_msdk.utils import flatten_json

class ConfigurableOAuthAuthenticator(OAuthAuthenticator):

    @property
    def oauth_request_body(self) -> dict:
        """Build up a list of OAuth2 parameters to use depending
        on what configuration items have been set and the type of OAuth
        flow set by the grant_type.
        """

        client_id = self.config.get('client_id')
        client_secret = self.config.get('client_secret')
        username = self.config.get('username')
        password = self.config.get('password')
        refresh_token = self.config.get('refresh_token')
        grant_type = self.config.get('grant_type')
        scope = self.config.get('scope')
        redirect_uri = self.config.get('redirect_uri')
        oauth_extras = self.config.get('oauth_extras')

        oauth_params = {}

        # Test mandatory parameters based on grant_type
        if grant_type:
            oauth_params['grant_type'] = grant_type
        else:
            raise ValueError("Missing grant type for OAuth Token.")

        if grant_type == 'client_credentials':
            if not (client_id and client_secret):
                raise ValueError(
                    "Missing either client_id or client_secret for 'client_credentials' grant_type."
                )

        if grant_type == 'password':
            if not (username and password):
                raise ValueError("Missing either username or password for 'password' grant_type.")

        if grant_type == 'refresh_token':
            if not refresh_token:
                raise ValueError("Missing either refresh_token for 'refresh_token' grant_type.")

        # Add parameters if they are set
        if scope:
            oauth_params['scope'] = scope
        if client_id:
            oauth_params['client_id'] = client_id
        if client_secret:
            oauth_params['client_secret'] = client_secret
        if username:
            oauth_params['username'] = username
        if password:
            oauth_params['password'] = password
        if refresh_token:
            oauth_params['refresh_token'] = refresh_token
        if redirect_uri:
            oauth_params['redirect_uri'] = redirect_uri
        if oauth_extras:
            for k, v in oauth_extras.items():
                oauth_params[k] = v

        return oauth_params

class TapRestApiMsdk(Tap):
    """rest-api tap class."""

    name = "tap-rest-api-msdk"

    # Required for Authentication in tap.py
    tap_name = "tap-rest-api-msdk"

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
            description="the json key of the replication key. Note that this should "
            "be an incrementing integer or datetime object.",
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
            description="An optional initial starting date when using a date based "
            "replication key and there is no state available.",
        ),
        th.Property(
            "search_parameter",
            th.StringType,
            required=False,
            description="An optional search parameter name used for querying specific "
            "records from supported API's. The intend for this parameter is to continue "
            "incrementally processing from a previous state. Example last-updated. "
            "When combined with a previous state value may look like this example "
            "last-updated=gt2022-08-01:00:00:00. Note: The api_query_parameter must "
            "used with replication_key, where the replication_key is the schema "
            "representation of the search_parameter.",
        ),
        th.Property(
            "search_prefix",
            th.StringType,
            required=False,
            description="An optional search value prefix which may be used by supported API's "
            "for incremental replication, e.g. eq, gt, or lt. The prefix values represent Equal "
            "to the provided value, Greater than, or Less than. An example when combined with "
            "a date value gt2022-08-01:00:00:00 returns records greater than this set date.",
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
            default='no_auth',
            required=False,
            description="The method of authentication used by the API. Supported options include "
            "oauth: for OAuth2 authentication, basic: Basic Header authorization - base64-encoded "
            "username + password config items, api_key: for API Keys in the header e.g. X-API-KEY,"
            " bearer_token: for Bearer token authorization. Defaults to no_auth which will take "
            "authentication parameters passed via the headers config."
        ),
        th.Property(
            "api_keys",
            th.ObjectType(),
            required=False,
            description="A object of API Key/Value pairs used by the api_key auth method "
            "Example: { ""X-API-KEY"": ""my secret value""}."
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The public application ID that's "
            "assigned for Authentication. The client_id should accompany a client_secret."
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The client_secret is a secret "
            "known only to the application and the authorization server. It is essential the "
            "application's own password."
        ),     
        th.Property(
            "username",
            th.StringType,
            required=False,
            description="Used for a number of authentication methods that use a user "
            "password combination for authentication."
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="Used for a number of authentication methods that use a user "
            "password combination for authentication."
        ),
        th.Property(
            "bearer_token",
            th.StringType,
            required=False,
            description="Used for the Bearer Authentication method, which uses a token "
            "as part of the authorization header for authentication."
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=False,
            description="An OAuth2 Refresh Token is a string that the OAuth2 client can use to "
            "get a new access token without the user's interaction."
        ),
        th.Property(
            "grant_type",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The grant_type is required "
            "to describe the OAuth2 flow. Flows support by this tap include client_credentials, "
            "refresh_token, password."
        ),
        th.Property(
            "scope",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The scope is optional, "
            "it is a mechanism to limit the amount of access that is granted to an access token. "
            "One or more scopes can be provided delimited by a space."
        ),
        th.Property(
            "access_token_url",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. This is the end-point for "
            "the authentication server used to exchange the authorization codes for a access "
            "token."
        ),
        th.Property(
            "redirect_uri",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. This is optional as the "
            "redirect_uri may be part of the token returned by the authentication server. If a "
            "redirect_uri is provided, it determines where the API server redirects the user "
            "after the user completes the authorization flow."
        ),
        th.Property(
            "oauth_extras",
            th.ObjectType(),
            required=False,
            description="A object of Key/Value pairs for additional oauth config parameters "
            "which may be required by the authorization server."
            "Example: { ""resource"": ""https://analysis.windows.net/powerbi/api""}."
        ),
        th.Property(
            "oauth_expiration_secs",
            th.IntegerType,
            default=None,
            required=False,
            description="Used for OAuth2 authentication method. This optional setting is a "
            "timer for the expiration of a token in seconds. If not set the OAuth will use "
            "the default expiration set in the token by the authorization server."
        ),
        th.Property(
            "next_page_token_path",
            th.StringType,
            default="$.next_page",
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
            "pagination_page_size",
            th.IntegerType,
            default=None,
            required=False,
            description="the size of each page in records. Defaults to None",
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
            replication_key=stream.get(
                        "replication_key", self.config.get("replication_key", "")
                    )
            search_parameter=stream.get(
                        "search_parameter", self.config.get("search_parameter", "")
                    )
            search_prefix=stream.get(
                        "search_prefix", self.config.get("search_prefix", "")
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
                    next_page_token_path=self.config["next_page_token_path"],
                    pagination_request_style=self.config["pagination_request_style"],
                    pagination_response_style=self.config["pagination_response_style"],
                    pagination_page_size=self.config.get("pagination_page_size"),
                    schema=schema,
                    start_date=start_date,
                    search_parameter=search_parameter,
                    search_prefix=search_prefix,
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
        # todo: this request format is not very robust

        auth_method = self.config.get('auth_method', '')
        if auth_method and not auth_method == 'no_auth':
            # Initializing Authenticator required in TAP for to dynamically discover schema
            authenticator = self.authenticator
            if authenticator:
                headers.update(authenticator.auth_headers or {})
                params.update(authenticator.auth_params or {})

        r = requests.get(self.config["api_url"] + path, params=params, headers=headers)
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

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        """Calls an appropriate SDK Authentication method based on the the set auth_method.
        If an auth_method is not provided, the tap will call the API using any settings from
        the headers and params config.
        Note: Each auth method requires certain configuration to be present see README.md
        for each auth methods configuration requirements.

        Raises:
            ValueError: if the auth_method is unknown.

        Returns:
            A SDK Authenticator or None if no auth_method supplied.
        """

        auth_method = self.config.get('auth_method', "")
        api_keys = self.config.get('api_keys', '')

        # Using API Key Authenticator, keys are extracted from api_keys dict
        if auth_method == "api_key":
            if api_keys:
                for k, v in api_keys.items():
                    key = k
                    value = v
            return APIKeyAuthenticator(
                stream=self,
                key=key,
                value=value
            )
        # Using Basic Authenticator
        elif auth_method == "basic":
            return BasicAuthenticator(
                stream=self,
                username=self.config.get('username', ''),
                password=self.config.get('password', '')
            )
        # Using OAuth Authenticator
        elif auth_method == "oauth":
            return ConfigurableOAuthAuthenticator(
                stream=self,
                auth_endpoint=self.config.get('access_token_url', ''),
                oauth_scopes=self.config.get('scope', ''),
                default_expiration=self.config.get('oauth_expiration_secs', ''),
            )
        # Using Bearer Token Authenticator
        elif auth_method == "bearer_token":
            return BearerTokenAuthenticator(
                stream=self,
                token=self.config.get('bearer_token', ''),
            )
        else:
            raise ValueError(
                f"Unknown authentication method {auth_method}. Use api_key, basic, oauth, or bearer_token."
            )
