"""REST API pagination handling."""

from typing import Any, Optional, cast
from urllib.parse import parse_qs, urlparse

import requests
from dateutil.parser import parse
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import (
    BaseOffsetPaginator,
    BasePageNumberPaginator,
    HeaderLinkPaginator,
)
from tap_rest_api_msdk.utils import unnest_dict


class RestAPIBasePageNumberPaginator(BasePageNumberPaginator):
    """REST API Base Page Number Paginator."""

    def __init__(self, *args, jsonpath=None, **kwargs):
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
    """REST API Offset Paginator."""

    def __init__(
        self, *args, jsonpath=None, pagination_total_limit_param: str, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.jsonpath = jsonpath
        self.pagination_total_limit_param = pagination_total_limit_param

    def has_more(self, response: requests.Response):
        """Return True if there are more pages to fetch.

        Args:
            response: The most recent response object.
            jsonpath: An optional jsonpath to where the tokens are located in
                      the response, defaults to pagination in the response.

        Returns:
            Whether there are more pages to fetch.

        """
        if self.jsonpath:
            pagination = next(extract_jsonpath(self.jsonpath, response.json()), None)
        else:
            pagination = response.json().get("pagination", None)
        if pagination:
            pagination = unnest_dict(pagination)

        if pagination and all(x in pagination for x in ["offset", "limit"]):
            record_limit = pagination.get(self.pagination_total_limit_param, 0)
            records_read = pagination["offset"] + pagination["limit"]
            if records_read <= record_limit:
                return True

        return False


class SimpleOffsetPaginator(BaseOffsetPaginator):
    """Simple Offset Paginator."""

    def __init__(self, *args, pagination_page_size: int = 25, **kwargs):
        super().__init__(*args, **kwargs)
        self._pagination_page_size = pagination_page_size

    def has_more(self, response: requests.Response):
        """Return True if there are more pages to fetch.

        Args:
            response: The most recent response object.

        Returns:
            Whether there are more pages to fetch.

        """
        return len(response.json()) == self._pagination_page_size


class RestAPIHeaderLinkPaginator(HeaderLinkPaginator):
    """REST API Header Link Paginator."""

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

    def get_next_url(self, response: requests.Response) -> Optional[Any]:
        """Return next page parameter(s).

        Return next page parameter(s) for identifying the next page
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
                cast(int, self._page_count) * self.pagination_page_size
                >= self.pagination_results_limit
            )
        ):
            return None

        # Leverage header links returned by the GitHub API.
        if "next" not in response.links.keys():
            return None

        # Exit early if there is no URL in the next links
        if not response.links.get("next", {}).get("url"):
            return None

        resp_json = response.json()
        if isinstance(resp_json, list):
            results = resp_json
        else:
            results = resp_json.get("items")

        # Exit early if the response has no items. ? Maybe duplicative the "next" link
        # check.
        if not results:
            return None

        # Unfortunately endpoints such as /starred, /stargazers, /events and /pulls do
        # not support the "since" parameter out of the box. So we use a workaround here
        # to exit early. For such streams, we sort by descending dates (most recent
        # first), and paginate "back in time" until we reach records before our
        # "fake_since" parameter.
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

            # commit_timestamp is a constructed key which does not exist in the raw
            # response
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
            return parsed_url.query

        return None
