"""REST client handling, including RestApiStream base class."""

from pathlib import Path

from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class RestApiStream(RESTStream):
    """rest-api stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]
