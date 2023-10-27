"""REST client handling, including RestApiStream base class."""

from pathlib import Path
from typing import Any

from singer_sdk.streams import RESTStream
from tap_rest_api_msdk.auth import get_authenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class RestApiStream(RESTStream):
    """rest-api stream class."""

    # Intialise self.http_auth used by prepare_request
    http_auth = None
    # Cache the authenticator using a Smart Singleton pattern
    try:
        self.assigned_authenticator
    except NameError:
        _authenticator = None
    else:
        if self.assigned_authenticator:  # noqa: F821
            _authenticator = self.assigned_authenticator  # noqa: F821

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
