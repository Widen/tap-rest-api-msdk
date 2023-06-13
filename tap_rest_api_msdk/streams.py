"""Stream type classes for tap-rest-api-msdk."""

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, cast

import requests
import email.utils
from singer_sdk.pagination import SinglePagePaginator, BaseOffsetPaginator, BaseHATEOASPaginator, JSONPathPaginator, HeaderLinkPaginator, SimpleHeaderPaginator, BasePageNumberPaginator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_rest_api_msdk.client import RestApiStream
from tap_rest_api_msdk.utils import flatten_json
from urllib.parse import urlparse, parse_qsl, parse_qs


class RestAPIBasePageNumberPaginator(BasePageNumberPaginator):
    def __init__(
        self,
        *args,
        jsonpath: str = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._jsonpath = jsonpath

    def has_more(self, response: requests.Response):
        """Return True if there are more pages to fetch.

        Args:
            response: The most recent response object.
            jsonpath: An optional jsonpath to where the tokens are located in
                      the response, defaults to `hasMore` in the response.

        Returns:
            Whether there are more pages to fetch.
        """
        
        if self._jsonpath:
            return next(extract_jsonpath(self._jsonpath, response.json()), None)
        else:
            return response.json().get("hasMore", None)

class RestAPIOffsetPaginator(BaseOffsetPaginator):
    def __init__(
        self,
        *args,
        jsonpath: str = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._jsonpath = jsonpath

    def has_more(self, response: requests.Response):
        """Return True if there are more pages to fetch.

        Args:
            response: The most recent response object.
            jsonpath: An optional jsonpath to where the tokens are located in
                      the response, defaults to pagination in the response.

        Returns:
            Whether there are more pages to fetch.
        """
        
        if self._jsonpath:
            pagination = next(extract_jsonpath(self._jsonpath, response.json()), None)
        else:
            pagination = response.json().get("pagination", None)

        if pagination and all(x in pagination for x in ["offset", "limit"]):
            record_limit = pagination.get("total",pagination.get("count",0))
            records_read = pagination["offset"] + pagination["limit"]
            if records_read <= record_limit:
                return True

        return False

class RestAPIHeaderLinkPaginator(HeaderLinkPaginator):
    def __init__(
        self,
        *args,
        pagination_page_size: int = 25,
        pagination_results_limit: Optional[int] = None,
        use_fake_since_parameter: Optional[bool] = False,
        replication_key: Optional[str] = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.pagination_page_size = pagination_page_size
        self.pagination_results_limit = pagination_results_limit
        self.use_fake_since_parameter = use_fake_since_parameter
        self.replication_key = replication_key

    def get_next_url(
        self, response: requests.Response
    ) -> Optional[Any]:
        """Return next page parameter(s) for identifying the next page
           or None if no more pages.
           
           Logic based on https://github.com/MeltanoLabs/tap-github
           
        Args:
            response: The most recent response object.
            pagination_page_size: A limit for records per page. Default=25
            pagination_results_limit: A limit to the number of pages returned
            use_fake_since_parameter: A work around for GitHub. default=False
            replication_key: Key for incremental processing

        Returns:
            Page Parameters if there are more pages to fetch, else None.
        """
        # Exit if the set Record Limit is reached.
        if (
            self._page_count
            and self.pagination_results_limit
            and (
                cast(int, self._page_count) * self.pagination_page_size >= self.pagination_results_limit
            )
        ):
            return None
          
        # Leverage header links returned by the GitHub API.
        if "next" not in response.links.keys():
            return None

        # Exit early if there is no URL in the next links
        if not response.links.get("next",{}).get("url"):
            return None

        resp_json = response.json()
        if isinstance(resp_json, list):
            results = resp_json
        else:
            results = resp_json.get("items")

        # Exit early if the response has no items. ? Maybe duplicative the "next" link check.
        if not results:
            return None
          
        # Unfortunately endpoints such as /starred, /stargazers, /events and /pulls do not support
        # the "since" parameter out of the box. So we use a workaround here to exit early.
        # For such streams, we sort by descending dates (most recent first), and paginate
        # "back in time" until we reach records before our "fake_since" parameter.
        if self.replication_key and self.use_fake_since_parameter:
            request_parameters = parse_qs(str(urlparse(response.request.url).query))
            # parse_qs interprets "+" as a space, revert this to keep an aware datetime
            try:
                since = (
                    request_parameters["fake_since"][0].replace(" ", "+")
                    if "fake_since" in request_parameters
                    else ""
                )
            except IndexError:
                return None

            direction = (
                request_parameters["direction"][0]
                if "direction" in request_parameters
                else None
            )

            # commit_timestamp is a constructed key which does not exist in the raw response
            replication_date = (
                results[-1][self.replication_key]
                if self.replication_key != "commit_timestamp"
                else results[-1]["commit"]["committer"]["date"]
            )
            # exit early if the replication_date is before our since parameter
            if (
                since
                and direction == "desc"
                and (parse(replication_date) < parse(since))
            ):
                return None

        # Use header links returned by the API to return the query parameters.
        parsed_url = urlparse(response.links["next"]["url"])

        if parsed_url.query:
            return(parsed_url.query)
          
        return None


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
        pagination_results_limit: Optional[int] = None,
        pagination_next_page_param: Optional[str] = None,
        pagination_limit_per_page_param: Optional[str] = None,
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
            pagination_results_limit: see tap.py
            pagination_next_page_param: see tap.py
            pagination_limit_per_page_param: see tap.py
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
        if next_page_token_path:
            self.next_page_token_jsonpath = next_page_token_path
        elif pagination_request_style == 'jsonpath_paginator' or pagination_request_style == 'default':
            self.next_page_token_jsonpath = "$.next_page" # Set default for jsonpath_paginator
        get_url_params_styles = {"style1": self._get_url_params_offset_style,
                                 "offset": self._get_url_params_offset_style,
                                 "page": self._get_url_params_page_style,
                                 "header_link": self._get_url_params_header_link,
                                 "hateoas_body": self._get_url_params_hateoas_body}
        self.get_url_params = get_url_params_styles.get(  # type: ignore
            pagination_response_style, self._get_url_params_page_style
        ) # Defaults to page_style url_params
        self.pagination_request_style = pagination_request_style
        self.pagination_results_limit = pagination_results_limit
        self.pagination_next_page_param = pagination_next_page_param
        self.pagination_limit_per_page_param = pagination_limit_per_page_param
        self.start_date = start_date
        self.search_parameter = search_parameter
        self.search_prefix = search_prefix
        
        # Setting Pagination Limits
        if self.pagination_request_style == 'restapi_header_link_paginator':
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                if self.pagination_limit_per_page_param:
                    page_limit_param = self.pagination_limit_per_page_param
                else:
                    page_limit_param = "per_page"
                self.pagination_page_size = int(self.params.get(page_limit_param, 25)) # Default to requesting 25 records
        elif self.pagination_request_style == 'style1' or self.pagination_request_style == 'offset_paginator':
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit # Will raise an exception.
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                if self.pagination_limit_per_page_param:
                    page_limit_param = self.pagination_limit_per_page_param
                else:
                    page_limit_param = "limit"
                self.pagination_page_size = int(self.params.get(page_limit_param, 25)) # Default to requesting 25 records
        else:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit # Will raise an exception.
            self.pagination_page_size = pagination_page_size

        # GitHub is missing the "since" parameter on a few endpoints
        # set this parameter to True if your stream needs to navigate data in descending order
        # and try to exit early on its own.
        # This only has effect on streams whose `replication_key` is `updated_at`.
        self.use_fake_since_parameter = False

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

    def get_new_paginator(self):
        """Return the requested paginator required to retrieve all data from the API.

        Returns:
              Paginator Class.

        """

        self.logger.info(f"the next_page_token_jsonpath = {self.next_page_token_jsonpath}.")

        if self.pagination_request_style == 'jsonpath_paginator' or self.pagination_request_style == 'default':
            return JSONPathPaginator(self.next_page_token_jsonpath)
        elif self.pagination_request_style == 'simple_header_paginator': # Example Gitlab.com
            return SimpleHeaderPaginator('X-Next-Page')
        elif self.pagination_request_style == 'header_link_paginator':
            return HeaderLinkPaginator()
        elif self.pagination_request_style == 'restapi_header_link_paginator': # Example GitHub.com
            return RestAPIHeaderLinkPaginator(pagination_page_size=self.pagination_page_size,
                                              pagination_results_limit=self.pagination_results_limit,
                                              replication_key=self.replication_key)
        elif self.pagination_request_style == 'style1' or self.pagination_request_style == 'offset_paginator':
            return RestAPIOffsetPaginator(start_value=1,
                                          page_size=self.pagination_page_size,
                                          jsonpath=self.next_page_token_jsonpath)
        elif self.pagination_request_style == 'hateoas_paginator':
            return BaseHATEOASPaginator()
        elif self.pagination_request_style == 'single_page_paginator':
            return SinglePagePaginator()
        elif self.pagination_request_style == 'page_number_paginator':
            return RestAPIBasePageNumberPaginator(jsonpath=self.next_page_token_jsonpath)
        else:
            self.logger.error(f"Unknown paginator {self.pagination_request_style}. Please declare a valid paginator.")
            raise ValueError(
                f"Unknown paginator {self.pagination_request_style}. Please declare a valid paginator."
            )

    def _get_url_params_page_style(
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
            if self.pagination_next_page_param:
                next_page_parm = self.pagination_next_page_param
            else:
                next_page_parm = "page"
            params[next_page_parm] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def _get_url_params_offset_style(
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
            if self.pagination_next_page_param:
                next_page_parm = self.pagination_next_page_param
            else:
                next_page_parm = "offset"
            params[next_page_parm] = next_page_token
        if self.pagination_page_size is not None:
            if self.pagination_limit_per_page_param:
                limit_per_page_param = self.pagination_limit_per_page_param
            else:
                limit_per_page_param = "limit"
            params[limit_per_page_param] = self.pagination_page_size
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params
      
    def _get_url_params_header_link(
        self, context: Optional[Dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.
        Logic based on https://github.com/MeltanoLabs/tap-github

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
        if self.pagination_page_size:
            pagination_page_size = self.pagination_page_size
        else:
            pagination_page_size = 25 # Default to 25 per page if not set
        if self.pagination_limit_per_page_param:
            limit_per_page_param = self.pagination_limit_per_page_param
        else:
            limit_per_page_param = "per_page"
        params[limit_per_page_param] = pagination_page_size
        if next_page_token:
            request_parameters = parse_qs(str(next_page_token))
            for k, v in request_parameters.items():
                params[k] = v            

        if self.replication_key == "updated_at":
            params["sort"] = "updated"
            params["direction"] = "desc" if self.use_fake_since_parameter else "asc"

        # Unfortunately the /starred, /stargazers (starred_at) and /events (created_at) endpoints do not support
        # the "since" parameter out of the box. But we use a workaround in 'get_next_page_token'.
        elif self.replication_key in ["starred_at", "created_at"]:
            params["sort"] = "created"
            params["direction"] = "desc"

        # Warning: /commits endpoint accept "since" but results are ordered by descending commit_timestamp
        elif self.replication_key == "commit_timestamp":
            params["direction"] = "desc"

        elif self.replication_key:
            self.logger.warning(
                f"The replication key '{self.replication_key}' is not fully supported by this client yet."
            )

        since = self.get_starting_timestamp(context)
        since_key = "since" if not self.use_fake_since_parameter else "fake_since"
        if self.replication_key and since:
            params[since_key] = since
            # Leverage conditional requests to save API quotas
            # https://github.community/t/how-does-if-modified-since-work/139627
            self._http_headers["If-modified-since"] = email.utils.format_datetime(since)

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


            HATEOAS stands for "Hypermedia as the Engine of Application State".
             See https://en.wikipedia.org/wiki/HATEOAS.            

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
                
        # Set Pagination Limits if required.
        if self.pagination_page_size and self.pagination_limit_per_page_param:
            params[pagination_limit_per_page_param] = self.pagination_page_size
                
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
