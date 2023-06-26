"""REST client handling, including RestApiStream base class."""

from pathlib import Path
from typing import Any, Optional
from singer_sdk.authenticators import APIAuthenticatorBase, SimpleAuthenticator
from tap_rest_api_msdk.auth import select_authenticator
import requests

from singer_sdk.streams import RESTStream

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
    def authenticator(self) -> APIAuthenticatorBase:
        """Calls an appropriate SDK Authentication method based on the the set auth_method
        which is set in the config.
        If an authenticator (auth_method) is not specified, REST-based taps will simply pass
        `http_headers` as defined in the stream class.
        
        Note: Each auth method requires certain configuration to be present see README.md
        for each auth methods configuration requirements.

        Raises:
            ValueError: if the auth_method is unknown.

        Returns:
            A SDK Authenticator or SimpleAuthenticator if no auth_method supplied.
        """

        stream_authenticator = select_authenticator(self)
        
        if stream_authenticator:
            return stream_authenticator
        else:
            return SimpleAuthenticator(stream=self)
          

