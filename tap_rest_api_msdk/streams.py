"""Stream type classes for tap-rest-api-msdk."""

from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_rest_api_msdk.client import RestApiStream
from tap_rest_api_msdk.utils import flatten_json
from urllib.parse import urlparse, parse_qsl


class DynamicStream(RestApiStream):
    """Define custom stream."""

    def __init__(
        self,
        tap: Any,
        name: str,
        records_path: str,
        path: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        primary_keys: Optional[list] = None,
        replication_key: Optional[str] = None,
        except_keys: Optional[list] = None,
        next_page_token_path: Optional[str] = None,
        schema: Optional[dict] = None,
        pagination_request_style: str = "default",
        pagination_response_style: str = "default",
        pagination_page_size: Optional[int] = None,
        start_date: Optional[datetime] = None,
        search_parameter: Optional[str] = None,
        search_prefix: Optional[str] = None,
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
            start_date: see tap.py
            search_parameter: see tap.py
            search_prefix: see tap.py

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
        get_url_params_styles = {"style1": self._get_url_params_style1,
                                 "hateoas_body": self._get_url_params_hateoas_body}
        self.get_url_params = get_url_params_styles.get(  # type: ignore
            pagination_response_style, self._get_url_params_default
        )
        get_next_page_token_styles = {"style1": self._get_next_page_token_style1,
                                      "hateoas_body": self._get_next_page_token_hateoas_body}
        self.get_next_page_token = get_next_page_token_styles.get(  # type: ignore
            pagination_response_style, self._get_next_page_token_default
        )
        self.start_date = start_date
        self.search_parameter = search_parameter
        self.search_prefix = search_prefix

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

    def _get_next_page_token_hateoas_body(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        """Return a token for identifying next page or None if no more pages.

        This method follows method of calculating the next page token from the
        API response body itself following HATEOAS Rest model.


        HATEOAS stands for "Hypermedia as the Engine of Application State". See
        https://en.wikipedia.org/wiki/HATEOAS.

        Args:
            response: required - the requests.Response given by the api call.
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

            # Setting up the url using next_page_token parameters
            # for subsequent calls.
            url_parsed = urlparse(next_page_token)
            if url_parsed.path == next_page_token:
                self.path = ""
            else:
                self.path=url_parsed.path

        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

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

    def get_start_date(
        self, context: dict
    ) -> Any:
        """Returns a start date if a DateTime bookmark is available.

        Args:
            context: - the singer context object.

        Returns:
            An start date else and empty string.

        """
        try:
            return self.get_starting_timestamp(context).strftime("%Y-%m-%dT%H:%M:%S")
        except (ValueError, AttributeError):
            return ""

    def _get_url_params_hateoas_body(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: optional - the singer context object.
            next_page_token: optional - the token for the next page of results.


            HATEOAS stands for "Hypermedia as the Engine of Application State". See
            https://en.wikipedia.org/wiki/HATEOAS.            

            Note: Under the HATEOAS model, the returned token contains all the 
            required parameters for the subsequent call. The function splits the
            parameters into Dict key value pairs for subsequent requests.

        Returns:
            An object containing the parameters to add to the request.

        """

        start_date = self.get_start_date(context)

        bookmark = self.get_starting_replication_key_value(context)

        params: dict = {}
        if self.params:
            for k, v in self.params.items():
                params[k] = v
        if next_page_token:
            # Parse the next_page_token for the path and parameters
            url_parsed = urlparse(next_page_token)
            if url_parsed.query:
                params.update(parse_qsl(url_parsed.query))
            else:
                params.update(parse_qsl(url_parsed.path))
            if url_parsed.path == next_page_token:
                self.path = ""
            else:
                self.path=url_parsed.path
        elif self.replication_key:
            # Setup initial replication start_date or provided identifier
            if self.search_parameter and start_date:
                params[self.search_parameter] = self.search_prefix + start_date
            elif self.search_parameter and bookmark:
                params[self.search_parameter] = self.search_prefix + bookmark

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
