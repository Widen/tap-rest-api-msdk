"""REST client handling, including RestApiStream base class."""

import os
from requests_aws4auth import AWS4Auth
import boto3
from pathlib import Path
from typing import Any
from singer_sdk.authenticators import APIAuthenticatorBase
import requests

from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class AWSAuthenticator(APIAuthenticatorBase):
  
    def __init__(
        self,
        stream: RESTStream,
        http_auth = None,
    ) -> None:
        """Create a new AWSAuthenticator extending the APIAuthenticatorBase.

        If auths is provided, it will be added to the PreparedRequest
        for the stream.

        Args:
            stream: The stream instance to use with this authenticator.
            auth: AWS4Auth object.
        """
        super().__init__(stream=stream)

        # TODO: Add logic to set stream.http_auth


class RestApiStream(RESTStream):
    """rest-api stream class."""

    @property
    def url_base(self) -> Any:
        """Return the API URL root, configurable via tap settings.

        Returns:
            The base url for the api call.

        """
        return self.config["api_url"]

class AWSConnectClient:
    """A connection class to AWS Resources"""

    def __init__(
            self,
            connection_config,
            create_signed_credentials: bool = True
        ):
        self.connection_config = connection_config
        

        # Initialise the variables
        self.create_signed_credentials = create_signed_credentials
        self.aws_auth = None
        self.region = None
        self.credentials = None
        self.aws_service = None
        
        # Establish a AWS Client
        self.credentials = self._create_aws_client()
        
        # Store AWS Signed Credentials
        self._store_aws4auth_credentials()


    def _create_aws_client(self, config=None):
        if not config:
            config = self.connection_config

        # Get the required parameters from config file and/or environment variables
        aws_profile = config.get('aws_profile') or os.environ.get('AWS_PROFILE')
        aws_access_key_id = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')
        aws_region = config.get('aws_region') or os.environ.get('AWS_REGION')
        self.aws_service = config.get('aws_service',None) or os.environ.get('AWS_SERVICE')
        
        if not config.get('create_signed_credentials',None):
            self.create_signed_credentials = False

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=aws_region,
                aws_session_token=aws_session_token
            )
        # AWS Profile based authentication
        elif aws_profile:
            aws_session = boto3.session.Session(profile_name=aws_profile)
        else:
            aws_session = None
            
        if aws_session:
            self.region = aws_session.region_name
            return aws_session.get_credentials()
        else:
            return None

      
    def _store_aws4auth_credentials(self):
        """Stores the AWS Signed Credential for the available AWS credentials.

        Returns:
            The None.

        """

        if self.create_signed_credentials and self.credentials:
            self.aws_auth = AWS4Auth(self.credentials.access_key, self.credentials.secret_key, self.region, self.aws_service, aws_session=self.credentials.token)
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

        return aws_session.client(self.aws_service,
                                  region_name=self.region)
