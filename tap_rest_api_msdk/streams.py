"""Stream type classes for tap-rest-api-msdk."""

from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_rest_api_msdk.client import RestApiStream
from tap_rest_api_msdk.utils import flatten_json


class DynamicStream(RestApiStream):
    """Define custom stream."""

    def __init__(
        self,
        tap: Any,
        name: str,
        records_path: str,
        path: str,
        params: dict = None,
        headers: dict = None,
        primary_keys: list = None,
        replication_key: str = None,
        except_keys: list = None,
        next_page_token_path: str = None,
        schema: dict = None,
        pagination_request_style: str = "default",
        pagination_response_style: str = "default",
        pagination_page_size: int = None,
    ) -> None:
        """Class initialization.

        Args:
            tap: see tap.py
            name: see tap.py
            path: see tap.py
            params: see tap.py
            headers: see tap.py
            primary_keys: see tap.py
            replication_key: see tap.py
            except_keys: see tap.py
            records_path: see tap.py
            next_page_token_path: see tap.py
            schema: the json schema for the stream.
            pagination_request_style: see tap.py
            pagination_response_style: see tap.py
            pagination_page_size: see tap.py

        """
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
        self.next_page_token_jsonpath = (
            next_page_token_path  # Or override `get_next_page_token`.
        )
        self.pagination_page_size = pagination_page_size
        get_url_params_styles = {"style1": self._get_url_params_style1}
        self.get_url_params = get_url_params_styles.get(  # type: ignore
            pagination_response_style, self._get_url_params_default
        )
        get_next_page_token_styles = {"style1": self._get_next_page_token_style1}
        self.get_next_page_token = get_next_page_token_styles.get(  # type: ignore
            pagination_response_style, self._get_next_page_token_default
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
              A dictionary of the headers to be included in the request.

        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")

        if self.headers:
            for k, v in self.headers.items():
                headers[k] = v

        return headers

    def _get_next_page_token_default(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[str]:
        """Return a token for identifying next page or None if no more pages.

        This method follows the default style of getting the next page token from the
        default path provided in the config or, if that doesn't exist, the header.

        Args:
            response: the requests.Response given by the api call.
            previous_token: optional - the token representing the current/previous page
                of results.

        Returns:
              A str representing the next page to be queried or `None`.

        """
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def _get_next_page_token_style1(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        """Return a token for identifying next page or None if no more pages.

        This method follows method of calculating the next page token from the
        offsets, limits, and totals provided by the API.

        Args:
            response: required - the requests.Response given by the api call.
            previous_token: optional - the token representing the current/previous page
                of results.

        Returns:
              A str representing the next page to be queried or `None`.

        """
        pagination = response.json().get("pagination", {})
        if pagination and all(x in pagination for x in ["offset", "limit", "total"]):
            next_page_token = pagination["offset"] + pagination["limit"]
            if next_page_token <= pagination["total"]:
                return next_page_token
        return None

    def _get_url_params_default(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: optional - the singer context object.
            next_page_token: optional - the token for the next page of results.

        Returns:
            An object containing the parameters to add to the request.

        """
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

    def _get_url_params_style1(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: optional - the singer context object.
            next_page_token: optional - the token for the next page of results.

        Returns:
            An object containing the parameters to add to the request.

        """
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
        """Parse the response and return an iterator of result rows.

        Args:
            response: required - the requests.Response given by the api call.

        Yields:
              Parsed records.

        """
        yield from extract_jsonpath(self.records_path, input=response.json())

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: required - the record for processing.
            context: optional - the singer context object.

        Returns:
              A record that has been processed.

        """
        return flatten_json(row, self.except_keys)
