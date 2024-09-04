"""REST authentication handling."""

import os
from typing import Any

import boto3
from requests_aws4auth import AWS4Auth
from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    APIKeyAuthenticator,
    BasicAuthenticator,
    BearerTokenAuthenticator,
    OAuthAuthenticator,
)


class AWSConnectClient:
    """A connection class to AWS Resources."""

    def __init__(self, connection_config, create_signed_credentials: bool = True):
        self.connection_config = connection_config

        # Initialise the variables
        self.create_signed_credentials = create_signed_credentials
        self.aws_auth = None
        self.region = None
        self.credentials = None
        self.aws_service = None
        self.aws_session = None

        # Establish a AWS Client
        self.credentials = self._create_aws_client()

        # Store AWS Signed Credentials
        self._store_aws4auth_credentials()

    def _create_aws_client(self, config=None):
        if not config:
            config = self.connection_config

        # Get the required parameters from config file and/or environment variables
        aws_profile = config.get("aws_profile") or os.environ.get("AWS_PROFILE")
        aws_access_key_id = config.get("aws_access_key_id") or os.environ.get(
            "AWS_ACCESS_KEY_ID"
        )
        aws_secret_access_key = config.get("aws_secret_access_key") or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
        aws_session_token = config.get("aws_session_token") or os.environ.get(
            "AWS_SESSION_TOKEN"
        )
        aws_region = config.get("aws_region") or os.environ.get("AWS_REGION")
        self.aws_service = config.get("aws_service", None) or os.environ.get(
            "AWS_SERVICE"
        )

        if not config.get("create_signed_credentials", True):
            self.create_signed_credentials = False

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            self.aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=aws_region,
                aws_session_token=aws_session_token,
            )
        # AWS Profile based authentication
        elif aws_profile:
            self.aws_session = boto3.session.Session(profile_name=aws_profile)
        else:
            self.aws_session = None

        if self.aws_session:
            self.region = self.aws_session.region_name
            return self.aws_session.get_credentials()
        else:
            return None

    def _store_aws4auth_credentials(self):
        """Store the AWS Signed Credential for the available AWS credentials.

        Returns:
            The None.

        """
        if self.create_signed_credentials and self.credentials:
            self.aws_auth = AWS4Auth(
                self.credentials.access_key,
                self.credentials.secret_key,
                self.region,
                self.aws_service,
                aws_session=self.credentials.token,
            )
        else:
            self.aws_auth = None

    def get_awsauth(self):
        """Return the AWS Signed Connection for provided credentials.

        Returns:
            The awsauth object.

        """
        return self.aws_auth

    def get_aws_session_client(self):
        """Return the AWS Signed Connection for provided credentials.

        Returns:
            The an AWS Session Client.

        """
        return self.aws_session.client(self.aws_service, region_name=self.region)


class ConfigurableOAuthAuthenticator(OAuthAuthenticator):
    """Configurable OAuth Authenticator."""

    def get_initial_oauth_token(self):
        """Get oauth token for the tap schema discovery.

        Requests an oauth token and sets the auth headers.
        """
        if not self.is_token_valid():
            self.update_access_token()

        self.auth_headers["Authorization"] = f"Bearer {self.access_token}"

    @property
    def oauth_request_body(self) -> dict:
        """Build up a list of OAuth2 parameters.

        Build up a list of OAuth2 parameters to use depending
        on what configuration items have been set and the type of OAuth
        flow set by the grant_type.
        """
        # Test where the config is located in self
        if self.config:  # Tap Config
            my_config = self.config
        elif self._config:  # Stream Config
            my_config = self._config

        client_id = my_config.get("client_id")
        client_secret = my_config.get("client_secret")
        username = my_config.get("username")
        password = my_config.get("password")
        refresh_token = my_config.get("refresh_token")
        grant_type = my_config.get("grant_type")
        scope = my_config.get("scope")
        redirect_uri = my_config.get("redirect_uri")
        oauth_extras = my_config.get("oauth_extras")

        oauth_params = {}

        # Test mandatory parameters based on grant_type
        if grant_type:
            oauth_params["grant_type"] = grant_type
        else:
            raise ValueError("Missing grant type for OAuth Token.")

        if grant_type == "client_credentials":
            if not (client_id and client_secret):
                raise ValueError(
                    "Missing either client_id or client_secret for "
                    "'client_credentials' grant_type."
                )

        if grant_type == "password":
            if not (username and password):
                raise ValueError(
                    "Missing either username or password for 'password' grant_type."
                )

        if grant_type == "refresh_token":
            if not refresh_token:
                raise ValueError(
                    "Missing either refresh_token for 'refresh_token' grant_type."
                )

        # Add parameters if they are set
        if scope:
            oauth_params["scope"] = scope
        if client_id:
            oauth_params["client_id"] = client_id
        if client_secret:
            oauth_params["client_secret"] = client_secret
        if username:
            oauth_params["username"] = username
        if password:
            oauth_params["password"] = password
        if refresh_token:
            oauth_params["refresh_token"] = refresh_token
        if redirect_uri:
            oauth_params["redirect_uri"] = redirect_uri
        if oauth_extras:
            for k, v in oauth_extras.items():
                oauth_params[k] = v

        return oauth_params


def select_authenticator(self) -> Any:
    """Call an appropriate SDK Authentication method.

    Calls an appropriate SDK Authentication method based on the the set auth_method.
    If an auth_method is not provided, the tap will call the API using any settings from
    the headers and params config.
    Note: Each auth method requires certain configuration to be present see README.md
    for each auth methods configuration requirements.

    Raises:
        ValueError: if the auth_method is unknown.

    Returns:
        A SDK Authenticator or None if no auth_method supplied.

    """
    # Test where the config is located in self
    if self.config:  # Tap Config
        my_config = self.config
    elif self._config:  # Stream Config
        my_config = self._config

    auth_method = my_config.get("auth_method", "")
    api_keys = my_config.get("api_keys", "")
    self.http_auth = None

    # Set http headers if headers are supplied
    # Some OAUTH2 API's require headers to be supplied
    # In the OAUTH request.
    auth_headers = my_config.get("headers", None)

    # Using API Key Authenticator, keys are extracted from api_keys dict
    if auth_method == "api_key":
        if api_keys:
            for k, v in api_keys.items():
                key = k
                value = v
        return APIKeyAuthenticator(stream=self, key=key, value=value)
    # Using Basic Authenticator
    elif auth_method == "basic":
        return BasicAuthenticator(
            stream=self,
            username=my_config.get("username", ""),
            password=my_config.get("password", ""),
        )
    # Using OAuth Authenticator
    elif auth_method == "oauth":
        return ConfigurableOAuthAuthenticator(
            stream=self,
            auth_endpoint=my_config.get("access_token_url", ""),
            oauth_scopes=my_config.get("scope", ""),
            default_expiration=my_config.get("oauth_expiration_secs", ""),
            oauth_headers=auth_headers,
        )
    # Using Bearer Token Authenticator
    elif auth_method == "bearer_token":
        return BearerTokenAuthenticator(
            stream=self,
            token=my_config.get("bearer_token", ""),
        )
    # Using AWS Authenticator
    elif auth_method == "aws":
        # Establish an AWS Connection Client and returned Signed Credentials
        self.aws_connection = AWSConnectClient(
            connection_config=my_config.get("aws_credentials", None)
        )

        if self.aws_connection.aws_auth:
            self.http_auth = self.aws_connection.aws_auth
        else:
            self.http_auth = None

        return self.http_auth
    elif auth_method != "no_auth":
        self.logger.error(
            f"Unknown authentication method {auth_method}. Use api_key, basic, oauth, "
            f"bearer_token, or aws."
        )
        raise ValueError(
            f"Unknown authentication method {auth_method}. Use api_key, basic, oauth, "
            f"bearer_token, or aws."
        )


def get_authenticator(self) -> Any:
    """Retrieve the appropriate authenticator in tap and stream.

    If the authenticator already exists, use the cached
    Authenticator

    Note: Store the authenticator in class variables used by the SDK.

    Returns:
        None

    """
    # Test where the config is located in self
    if self.config:  # Tap Config
        my_config = self.config
    elif self._config:  # Stream Config
        my_config = self._config

    auth_method = my_config.get("auth_method", None)
    self.http_auth = None

    if not self._authenticator:
        self._authenticator = select_authenticator(self)
        if not self._authenticator:
            # No Auth Method, use default Authenticator
            self._authenticator = APIAuthenticatorBase(stream=self)
    if auth_method == "oauth":
        if not self._authenticator.is_token_valid():
            # Obtain a new OAuth token as it has expired
            self._authenticator = select_authenticator(self)
    if auth_method == "aws":
        # Set the http_auth which is used in the Request call for AWS
        self.http_auth = self._authenticator
