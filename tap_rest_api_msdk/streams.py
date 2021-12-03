"""Stream type classes for tap-rest-api-msdk."""

import requests
from typing import Any, Dict, Optional, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_rest_api_msdk.client import RestApiStream
from tap_rest_api_msdk.utils import flatten_json


class DynamicStream(RestApiStream):
    """Define custom stream."""
    def __init__(
        self,
        tap,
        name,
        path=None,
        params=None,
        headers=None,
        primary_keys=None,
        replication_key=None,
        except_keys=None,
        records_path=None,
        next_page_token_path=None,
        schema=None,
        pagination_request_style='default',
        pagination_response_style='default',
        pagination_page_size=None,
    ):
        super().__init__(tap=tap, name=tap.name, schema=schema)

        if primary_keys is None:
            primary_keys = []

        self.name = name
        self.path = path
        self.params = params
        self.headers = headers
        self.primary_keys = primary_keys
        self.replication_key = replication_key
        self.except_keys = except_keys
        self.records_path = records_path
        self.next_page_token_jsonpath = next_page_token_path  # Or override `get_next_page_token`.
        self.pagination_page_size = pagination_page_size
        get_url_params_styles = {'style1': self._get_url_params_style1}
        self.get_url_params = get_url_params_styles.get(pagination_response_style, self._get_url_params_default)
        get_next_page_token_styles = {'style1': self._get_next_page_token_style1}
        self.get_next_page_token = get_next_page_token_styles.get(pagination_response_style,
                                                                  self._get_next_page_token_default)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")

        if self.headers:
            for k, v in self.headers.items():
                headers[k] = v

        return headers

    def _get_next_page_token_default(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath("self.next_page_token_jsonpath", response.json())
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def _get_next_page_token_style1(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        pagination = response.json().get('pagination', {})
        if pagination and all(x in pagination for x in ['offset', 'limit', 'total']):
            next_page_token = pagination['offset'] + pagination['limit']
            if next_page_token < pagination['total']:
                return next_page_token
        return None

    def _get_url_params_default(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.params:
            for k, v in self.params.items():
                params[k] = v
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def _get_url_params_style1(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.params:
            for k, v in self.params.items():
                params[k] = v
        if next_page_token:
            params["offset"] = next_page_token
        if self.pagination_page_size is not None:
            params["limit"] = self.pagination_page_size
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_path, input=response.json())

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return flatten_json(row, self.except_keys)
