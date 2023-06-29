"""REST client handling, including RestApiStream base class."""

from pathlib import Path
from typing import Any

from memoization import cached
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import RESTStream
from tap_rest_api_msdk.auth import select_authenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class RestApiStream(RESTStream):
    """rest-api stream class."""

    # Intialise self.http_auth used by prepare_request
    http_auth = None

    @property
    def url_base(self) -> Any:
        """Return the API URL root, configurable via tap settings.

        Returns:
            The base url for the api call.

        """
        return self.config["api_url"]
      

    @property
    @cached
    def authenticator(self) -> Any:
        """Calls an appropriate SDK Authentication method based on the the set auth_method
        which is set via the config.
        If an authenticator (auth_method) is not specified, REST-based taps will simply pass
        `http_headers` as defined in the tap and stream classes.
        
        Note 1: Each auth method requires certain configuration to be present see README.md
        for each auth methods configuration requirements.
        
        Note 2: The cached decorator will have all calls to the authenticator use the
        same instance logic to speed up processing. TODO: Examine if this affects OAuth2
        callbacks and if logic is required for callback for expiring AWS STS tokens.

        Raises:
            ValueError: if the auth_method is unknown.

        Returns:
            A SDK Authenticator or APIAuthenticatorBase if no auth_method supplied.
        """

        stream_authenticator = select_authenticator(self)
        
        if stream_authenticator:
            return stream_authenticator
        else:
            return APIAuthenticatorBase(stream=self)
          

