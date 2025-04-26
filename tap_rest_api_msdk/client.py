"""REST client handling, including RestApiStream base class."""

from pathlib import Path
from typing import Any, Optional
import logging
import requests

from singer_sdk.streams import RESTStream
from tap_rest_api_msdk.auth import get_authenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

# Configure logger
logger = logging.getLogger("tap-rest-api-msdk")

class RestApiStream(RESTStream):
    """rest-api stream class."""

    def __init__(self, *args, **kwargs):
        # Initialize our internal logger first
        self._stream_logger = logging.getLogger(f"tap-rest-api-msdk.{kwargs.get('name', 'stream')}")

        # Now call super() which will set up the SDK's logger property
        super().__init__(*args, **kwargs)

        self.http_auth = None
        self._authenticator = getattr(self, "assigned_authenticator", None)

    @property
    def url_base(self) -> Any:
        """Return the API URL root, configurable via tap settings.

        Returns:
            The base url for the api call.

        """
        return self.config["api_url"]

    @property
    def authenticator(self) -> Any:
        """Call an appropriate SDK Authentication method.

        Calls an appropriate SDK Authentication method based on the the set
        auth_method which is set via the config.
        If an authenticator (auth_method) is not specified, REST-based taps will simply
        pass `http_headers` as defined in the tap and stream classes.

        Note 1: Each auth method requires certain configuration to be present see
        README.md for each auth methods configuration requirements.

        Note 2: Using Singleton Pattern on the autenticator for caching with a check
        if an OAuth Token has expired and needs to be refreshed.

        Raises:
            ValueError: if the auth_method is unknown.

        Returns:
            A SDK Authenticator or APIAuthenticatorBase if no auth_method supplied.

        """
        # Obtaining Authenticator for authorisation to extract data.
        get_authenticator(self)

        return self._authenticator

    # Remove the conflicting logger property
    # We'll use the SDK's logger property which is set during initialization

    def request_decorator(self, func):
        """Decorate request function to add logging of requests and responses.

        Args:
            func: Request function to decorate

        Returns:
            Function: Decorated function
        """
        def wrapper(*args, **kwargs):
            # Log the request
            self.logger.info(f"Request: {args[0]} - {kwargs.get('params', {})}")

            # Call the original function - this returns a response, not a request
            response = func(*args, **kwargs)

            # Now we're working with the response object which has status_code
            if hasattr(response, 'status_code'):
                # Log the response status and headers without affecting the JSON output
                self.logger.info(f"Response status: {response.status_code}")
                self.logger.info(f"Response headers: {dict(response.headers)}")

                # Log a sample of the response body if it's JSON
                try:
                    if 'application/json' in response.headers.get('content-type', ''):
                        # Only log a small sample to avoid overwhelming logs
                        json_sample = str(response.json())[:500] + "..." if len(str(response.json())) > 500 else str(response.json())
                        self.logger.info(f"Response body sample: {json_sample}")
                except Exception:
                    self.logger.info("Response body: [Could not parse JSON]")

            return response

        return wrapper

    def prepare_request(self, context: Optional[dict], next_page_token: Optional[Any]) -> requests.PreparedRequest:
        """Prepare a request object for this REST stream.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        # First get the prepared request from the parent class
        prepared_request = super().prepare_request(context, next_page_token)

        # Log the request details
        self.logger.info(f"Preparing request: {prepared_request.method} {prepared_request.url}")
        if hasattr(prepared_request, 'headers') and prepared_request.headers:
            self.logger.info(f"Request headers: {dict(prepared_request.headers)}")

        # Try to log request body if it exists
        if hasattr(prepared_request, 'body') and prepared_request.body:
            try:
                body_sample = str(prepared_request.body)[:500] + "..." if len(str(prepared_request.body)) > 500 else str(prepared_request.body)
                self.logger.info(f"Request body: {body_sample}")
            except Exception as e:
                self.logger.warning(f"Could not log request body: {e}")

        return prepared_request
