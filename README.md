# tap-rest-api-msdk
![singer_rest_api_tap](https://user-images.githubusercontent.com/84364906/220881634-c0d0145a-ab85-44e9-91b6-e8d365da25f3.png)

`tap-rest-api-msdk` is a Singer tap for generic rest-apis. The novelty of this particular tap
is that it will auto-discover the stream's schema.

This is particularly useful if you have a stream with a very large and complex schema or
a stream that outputs records with varying schemas for each record. Can also be used for
simpler more reliable streams.

There are many forms of Authentication supported by this tap. By default for legacy support, you can pass Authentication via headers. If you want to use the built in support for Authentication, this tap supports
- Basic Authentication
- API Key
- Bearer Token
- OAuth
- AWS

Please note that OAuthJWTAuthentication has not been developed. If you are interested in contributing this, please fork and make a pull request.

Built with the Meltano [SDK](https://gitlab.com/meltano/sdk) for Singer Taps.

Gratitude goes to [anelendata](https://github.com/anelendata/tap-rest-api) for inspiring this "SDK-ized" version of their tap.

## Installation
If using via Meltano, add the following lines to your `meltano.yml` file and run the following command:

```yaml
plugins:
  extractors:
    - name: tap-rest-api-msdk
      namespace: tap_rest_api_msdk
      pip_url: tap-rest-api-msdk
      executable: tap-rest-api-msdk
      capabilities:
        - state
        - catalog
        - discover
      settings:
        - name: api_url
          kind: string
        - name: next_page_token_path
          kind: string
        - name: pagination_request_style
          kind: string
        - name: pagination_response_style
          kind: string
        - name: use_request_body_not_params
          kind: boolean
        - name: backoff_type
          kind: string
        - name: backoff_param
          kind: string
        - name: backoff_time_extension
          kind: integer
        - name: store_raw_json_message
          kind: boolean
        - name: pagination_page_size
          kind: integer
        - name: pagination_results_limit
          kind: integer
        - name: pagination_next_page_param
          kind: string
        - name: pagination_limit_per_page_param
          kind: string
        - name: pagination_total_limit_param
          kind: string
        - name: pagination_initial_offset
          kind: integer
        - name: streams
          kind: array
        - name: name
          kind: string
        - name: path
          kind: string
        - name: params
          kind: object
        - name: headers
          kind: object
        - name: records_path
          kind: string
        - name: primary_keys
          kind: array
        - name: replication_key
          kind: string
        - name: except_keys
          kind: array
        - name: num_inference_records
          kind: integer
        - name: start_date
          kind: date_iso8601
        - name: source_search_field
          kind: string
        - name: source_search_query
          kind: string
        - name: auth_method
          kind: string
        - name: api_key
          kind: object
        - name: client_id
          kind: password
        - name: client_secret
          kind: password
        - name: username
          kind: string
        - name: password
          kind: password
        - name: bearer_token
          kind: password
        - name: refresh_token
          kind: oauth
        - name: grant_type
          kind: string
        - name: scope
          kind: string
        - name: access_token_url
          kind: string
        - name: redirect_uri
          kind: string
        - name: oauth_extras
          kind: object
        - name: oauth_expiration_secs
          kind: integer
        - name: aws_credentials
          kind: object
```

```bash
meltano install extractor tap-rest-api-msdk
```

## Configuration

### Accepted Config Options


A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-rest-api-msdk --about
```

#### Top-level config options.
Parameters that appear at the stream-level will overwrite their top-level
counterparts except where noted in the stream-level params. Otherwise, the values
provided at the top-level will be the default values for each stream.:
- `api_url`: required: the base url/endpoint for the desired api.
- `pagination_request_style`: optional: style for requesting pagination, defaults to `default` which is the `jsonpath_paginator`, see Pagination below.
- `pagination_response_style`: optional: style of pagination results, defaults to `default` which is the `page` style response, see Pagination below.
- `use_request_body_not_params`: optional: sends the request parameters in the request body. This is normally not required, a few API's like OpenSearch require this. Defaults to `False`.
- `backoff_type`: optional: The style of Backoff [message|header] applied to rate limited APIs. Backoff times (seconds) come from response either the `message` or `header`. Defaults to `None`.
- `backoff_param`: optional: the header parameter to inspect for a backoff time. Defaults to `Retry-After`.
- `backoff_time_extension`: optional: An additional extension (seconds) to the backoff time over and above a jitter value - use where an API is not precise in it's backoff times. Defaults to `0`.
- `store_raw_json_message`: optional: An additional extension which will emit the whole message into an field `_sdc_raw_json`. Useful for a dynamic schema which cannot be automatically discovered. Defaults to `False`.
- `pagination_page_size`: optional: limit for size of page, defaults to None.
- `pagination_results_limit`: optional: limits the max number of records. Note: Will cause an exception if the limit is hit (except for the `restapi_header_link_paginator`). This should be used for development purposes to restrict the total number of records returned by the API. Defaults to None.
- `pagination_next_page_param`: optional: The name of the param that indicates the page/offset. Defaults to None.
- `pagination_limit_per_page_param`: optional: The name of the param that indicates the limit/per_page. Defaults to None.
- `pagination_total_limit_param`: optional: The name of the param that indicates the total limit e.g. total, count. Defaults to total
- `pagination_initial_offset`: optional: The initial offset for the first request. Defaults to 1.
- `next_page_token_path`: optional: a jsonpath string representing the path to the "next page" token. Defaults to `'$.next_page'` for the `jsonpath_paginator` paginator only otherwise None.
- `streams`: required: a list of objects that contain the configuration of each stream. See stream-level params below.
- `path`: optional: see stream-level params below.
- `params`: optional: see stream-level params below.
- `headers`: optional: see stream-level params below.
- `records_path`: optional: see stream-level params below.
- `primary_keys`: optional: see stream-level params below.
- `replication_key`: optional: see stream-level params below.
- `except_keys`: optional: see stream-level params below.
- `num_inference_keys`: optional: see stream-level params below.
- `start_date`: optional: see stream-level params below.
- `source_search_field`: optional: see stream-level params below.
- `source_search_query`: optional: see stream-level params below.
- `auth_method`: optional: see authentication params below.
- `api_key`: optional: see authentication params below.
- `client_id`: optional: see authentication params below.
- `client_secret`: optional: see authentication params below.
- `username`: optional: see authentication params below.
- `password`: optional: see authentication params below.
- `bearer_token`: optional: see authentication params below.
- `refresh_token`: optional: see authentication params below.
- `grant_type`: optional: see authentication params below.
- `scope`: optional: see authentication params below.
- `access_token_url`: optional: see authentication params below.
- `redirect_uri`: optional: see authentication params below.
- `oauth_extras`: optional: see authentication params below.
- `oauth_expiration_secs`: optional: see authentication params below.
- `aws_credentials`: optional: see authentication params below.

#### Stream level config options.
Parameters that appear at the stream-level
will overwrite their top-level counterparts except where noted below:
- `name`: required: name of the stream.
- `path`: optional: the path appended to the `api_url`.
- `params`: optional: an object of objects that provide the `params` in a `requests.get` method.
  Stream level params will be merged with top-level params with stream level params overwriting
  top-level params with the same key.
- `headers`: optional: an object of headers to pass into the api calls. Stream level
  headers will be merged with top-level params with stream level params overwriting
  top-level params with the same key
- `records_path`: optional: a jsonpath string representing the path in the requests response that contains the records to process. Defaults to `$[*]`.
- `primary_keys`: required: a list of the json keys of the primary key for the stream.
- `replication_key`: optional: the json key of the replication key. Note that this should be an incrementing integer or datetime object.
- `except_keys`: This tap automatically flattens the entire json structure and builds keys based on the corresponding paths.
  Keys, whether composite or otherwise, listed in this dictionary will not be recursively flattened, but instead their values will be
  turned into a json string and processed in that format. This is also automatically done for any lists within the records; therefore,
  records are not duplicated for each item in lists.
- `num_inference_keys`: optional: number of records used to infer the stream's schema. Defaults to 50.
- `schema`: optional: A valid Singer schema or a path-like string that provides
  the path to a `.json` file that contains a valid Singer schema. If provided,
  the schema will not be inferred from the results of an api call.
- `start_date`: optional: used by the the **offset**, **page**, and **hateoas_body** response styles. This is an initial starting date for an incremental replication if there is no
  existing state provided for an incremental replication. Example format 2022-06-10:23:10:10+1200.
- `source_search_field`: optional: used by the **offset**, **page**, and **hateoas_body** response style. This is a search/query parameter used by the API for an incremental replication.

  The difference between the `replication_key` and the `source_search_field` is the search field used in request parameters whereas the replication_key is the name of the field in the API reponse. Example if the source_search_field = **last-updated** the generated schema from the api discovery
  might be **meta_lastUpdated**. The replication_key is set to meta_lastUpdated, and the search_parameter to last-updated. Note: Please set the `replication_key`, `start_date`, `source_search_field`, and `source_search_query` parameters all together.
- `source_search_query`: optional: used by the **offset**, **page**, and **hateoas_body** response style. This is a query template to be issued against the API. A simple query template example for FHIR API's is **gt$last_run_date**.

  A more complex example against an Opensearch API, **{\\"bool\\": {\\"filter\\": [{\\"range\\": { \\"meta.lastUpdated\\": { \\"gt\\": \\"$last_run_date\\" }}}] }}**. Note: Any required double quotes in the query template must be escaped.

  At run-time, the tap will dynamically change the value **$last_run_date** with either the defined `start_date` parameter or the last bookmark / state value.
  Example: source_search_field=**last-updated**, the
  source_search_query = **gt$last_run_date**, and the current replication state = 2022-08-10:23:10:10+1200. At run time this creates a request parameter **last-updated=gt2022-06-10:23:10:10+1200**.

#### Top-Level Authentication config options.
- `auth_method`: optional: The method of authentication used by the API. Supported options
  include:
  - **oauth**: for OAuth2 authentication
  - **basic**: Basic Header authentication - base64-encoded username + password config items
  - **api_key**: for API Keys in the header e.g. X-API-KEY.
  - **bearer_token**: for Bearer token authentication.
  - **aws**: for AWS authentication. Works with the `aws_credentials` parameter.
  - Defaults to no_auth which will take authentication parameters passed via the headers config.
- `api_keys`: optional: A dictionary of API Key/Value pairs used by the api_key auth method
  Example: { "X-API-KEY": "my secret value"}.
- `client_id`: optional: Used for the OAuth2 authentication method. The public application ID
  that's assigned for Authentication. The **client_id** should accompany a **client_secret**.
- `client_secret`: optional: Used for the OAuth2 authentication method. The client_secret is a
  secret known only to the application and the authorization server. It is essential the
  application's own password.
- `username`: optional: Used for a number of authentication methods that use a user
  password combination for authentication.
- `password`: optional: Used for a number of authentication methods that use a user password
  combination for authentication.
- `bearer_token`: optional: Used for the Bearer Authentication method, which uses a token as part
  of the authorization header for authentication.
- `refresh_token`: optional: An OAuth2 Refresh Token is a string that the OAuth2 client can use to
  get a new access token without the user's interaction.
- `grant_type`: optional: Used for the OAuth2 authentication method. The grant_type is required
  to describe the OAuth2 flow. Flows support by this tap include **client_credentials**, **refresh_token**, **password**.
- `scope`: optional: Used for the OAuth2 authentication method. The scope is optional, it is a
  mechanism to limit the amount of access that is granted to an access token. One or more scopes
  can be provided delimited by a space.
- `access_token_url`: optional: Used for the OAuth2 authentication method. This is the end-point
  for the authentication server used to exchange the authorization codes for a access token.
- `redirect_uri`: optional: Used for the OAuth2 authentication method. This optional as the
  redirect_uri may be part of the token returned by the authentication server. If a redirect_uri
  is provided, it determines where the API server redirects the user after the user completes the
  authorization flow.
- `oauth_extras`: optional: A object of Key/Value pairs for additional oauth config parameters
  which may be required by the authorization server. Example: { "resource": "https://analysis.windows.net/powerbi/api" }.
- `oauth_expiration_secs`: optional: Used for OAuth2 authentication method. This optional setting
  is a timer for the expiration of a token in seconds. If not set the OAuth will use the default
  expiration set in the token by the authorization server.
- `aws_credentials`: optional: A object of Key/Value pairs to support AWS authentication when using the AWS authenticator. While the tap can use AWS [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html) environment variables and aws_profiles instead of supplying the keys and region, the [AWS service code](https://docs.aws.amazon.com/general/latest/gr/rande.html) needs to be specified e.g. `es` for OpenSearch / Elastic Search. By default the requirement to use `use_signed_credentials` is set to true. Config example:
  ```json
  { "aws_access_key_id": "my_aws_key_id",
    "aws_secret_access_key": "my_aws_secret_access_key",
    "aws_region": "us-east-1",
    "aws_service": "es",
    "use_signed_credentials": true}
  ```

#### Complex Authentication

The previous section showed out of the box methods for a single factor of authentication e.g. x-api-key, basic or oauth. If the API requires multiple forms of authentication, you may need to pass some of the authentication methods via the headers to be be combined with the main auth_method.

Example:
An API may use OAuth2 for authentication but also requires an X-API-KEY to be supplied as well. In this situation pass the X-API-KEY as part of the `headers` config, and the rest of the config should be set for OAuth e.g.

- headers = '{"x-api-key": "my_secret_api_key"}'
- auth_method = "oauth"
- grant_type = "client_credentials"
- access_token_url = "https://auth.example.server/oauth2/token"
- client_id = "my_example_client_id"
- client_secret = "my_example_client_secret"

Some servers may require additional information like a `Request-Context` which is usually Base64 encoded. If this is the case it should be included in the `headers` config as well.

Example:

- headers = '{"x-api-key": "my_secret_api_key", "Request-Context": "my_example_Base64_encoded_json_object"}'

## Pagination
API Pagination is a complex topic as there is no real single standard, and many different implementations.  Unless options are provided, both the request and results style type default to the `default`, which is the pagination style originally implemented. Where possible, this tap utilises the Meltano SDK paginators https://sdk.meltano.com/en/latest/reference.html#pagination .

### Default Request Style
The default request style for pagination is using a `JSONPath Paginator` to locate the next page token.

### Default Response Style
The default response style for pagination is described below:
- If there is a token, add that as a `page` URL parameter.

### Additional Request / Paginator Styles
There are additional request styles supported as follows for pagination.
- `jsonpath_paginator` or `default` - This style obtains the token for the next page from a specific location in the response body via JSONPath notation. In many situations the `jsonpath_paginator` is a more appropriate paginator to the `hateoas_paginator`.
  - `next_page_token_path` - The jsonpath to next page token. Example: `"$['@odata.nextLink']"`, this locates the token returned via the Microsoft Graph API. Default `'$.next_page'` for the `jsonpath_paginator` paginator only otherwise None.
- `offset_paginator` or `style1` - This style uses URL parameters named offset and limit
  - `offset` is calculated from the previous response, or not set if there is no previous response
  - `pagination_page_size` - Sets a limit to number of records per page / response. Default `25` records.
  - `pagination_limit_per_page_param` - the name of the API parameter to limit number of records per page. Default parameter name `limit`.
  - `pagination_total_limit_param` - The name of the param that indicates the total limit e.g. total, count. Defaults to total
  - `next_page_token_path` - Used to locate an appropriate link in the response. Default None - but looks in the `pagination` section of the JSON response by default. Example, jsonpath to get the offset from the NOAA API `'$.metadata.resultset'`.
  - `pagination_initial_offset` - The initial offset for the first request. Defaults to 1.
- `simple_header_paginator` - This style uses links in the Header Response to locate the next page. Example the `x-next-page` link used by the Gitlab API.
- `header_link_paginator` - This style uses the default header link paginator from the Meltano SDK.
- `restapi_header_link_paginator` - This style is a variant on the header_link_paginator. It supports the ability to read from GitHub API.
  - `pagination_page_size` - Sets a limit to number of records per page / response. Default `25` records.
  - `pagination_limit_per_page_param` - the name of the API parameter to limit number of records per page. Default parameter name `per_page`.
  - `pagination_results_limit` - Restricts the total number of records returned from the API. Default None i.e. no limit.
- `hateoas_paginator` - This style parses the next_token response for the parameters to pass. It is used by API's utilising the HATEOAS Rest style [HATEOAS](https://en.wikipedia.org/wiki/HATEOAS), including [FHIR API's](https://hl7.org/fhir/http.html).
  - `pagination_page_size` - Sets a limit to number of records per page / response. Default None.
  - `pagination_limit_per_page_param` - the name of the API parameter to limit number of records per page e.g. `_count` for [FHIR API's](https://hl7.org/fhir/http.html). Default None.
- `single_page_paginator` - A paginator that does works with single-page endpoints.
- `page_number_paginator` - Paginator class for APIs that use page number. Looks at the response link to determine more pages.
  - `next_page_token_path` - Use to locate an appropriate link in the response. Default `"hasMore"`.
- `simple_offset_paginator` - A paginator that uses `offset` and `limit` parameters to page through a collection of resources. Unlike `offset_paginator`, this paginator does not rely on any headers to determine whether it should keep paginating. Instead, it will continue paginating (by sending requests with increasing `offset`) until the API returns 0 results. You can use this paginator if the API returns a JSON array of records rather than a top-level object.
  - `pagination_page_size` - Sets a limit to number of records per page / response. Default `25` records.

### Additional Response Styles
There are additional response styles supported as follows.
- `default` or `page` - This style uses page style offsets params to identify the next page.
- `offset` or `style1` - This style retrieves pagination information by default from the `pagination` top-level element in the response.  Expected format is as follows:
    ```json
    "pagination": {
        "total": 136,
        "limit": 2,
        "offset": 2
    }
    ```
  The next page token, which in this case is really the next starting record number, is calculated by the limit, current offset, or None is returned to indicate no more data.  For this style, the response style _must_ include the limit in the response, even if none is specified in the request, as well as ( `total` or `count` ) and offset to calculate the next token value.

  It is expected that this API Response Style will be used with request style of `offset_paginator` or `style1`.
  - The `next_page_token_jsonpath` can be used to provide a JSONPath location to the pagination location e.g. `'$.metadata.resultset'`. Default `pagination` from the tap-level element in the response.
- `header_link` - This style parses the next page link in the Header Response. It is expected that this response will be used with an appropriate request style e.g. `restapi_header_link_paginator`.
  - `pagination_page_size` - Sets a limit to number of records per page / response. Default `25` records.
  - `pagination_limit_per_page_param` - the name of the API parameter to limit number of records per page. Default parameter name `per_page`.
  - `pagination_results_limit` - Restricts the total number of records returned from the API. Default None i.e. no limit.
- `hateoas_body` - This style requires a well crafted `next_page_token_path` configuration
  parameter to retrieve the request parameters from the GET request response for a subsequent request.

### JSON Path for extracting tokens
  The `next_page_token_path` and `records_path` use JSONPath to locate sections within the request reponse.

  The following example extracts the URL for the next pagination page.
    ```json
    "next_page_token_path": "$.link[?(@.relation=='next')].url."
    ```

  The following example demonstrates the power of JSONPath extensions by further splitting the URL and extracting just the parameters. Note: This is not required for FHIR API's but is provided for illustration of added functionality for complex use cases.
    ```json
    "next_page_token_path": "$.link[?(@.relation=='next')].url.`split(?, 1, 1)`"
    ```
  The [JSONPath Evaluator](https://jsonpath.com/) website is useful to test the correct json path expression to use.

  Example json response from a FHIR API.


    ```json
    {
      "resourceType": "Bundle",
      "id": "44f2zf06-g53c-4218-a3ef-08bb6c2fde4a",
      "meta": {
        "lastUpdated": "2022-06-28T18:25:01.165+12:00"
      },
      "type": "searchset",
      "total": 63,
      "link": [
        {
          "relation": "self",
          "url": "https://myexample_fhir_api_url/base_folder/ExampleService?_count=10&_getpageoffset=10&services-provided-type=MY_INITIAL_EXAMPLE_SERVICE"
        },
        {
          "relation": "next",
          "url": "https://myexample_fhir_api_url/base_folder?_getpages=44f2zf06-g53c-4218-a3ef-08bb6c2fde4a&_getpagesoffset=10&_count=10&_pretty=true&_bundletype=searchset"
        }
      ],
      "entry": [
        {
          "fullUrl": "https://myexample_fhir_api_url/base_folder/ExampleService/example-service-123456",
          "resource": {
            "resourceType": "ExampleService",
            "id": "example-service-123456"
          }
        }
      ]
  }
    ```

  Note: If you wish to extract the body from example GET request response above the following configuration parameter `records_path` will return the actual json content.
  ```json
  "records_path": "$.entry[*].resource"
  ```

## Example settings for different API's

This section provides examples of settings for accessing different API's. The tap configuration examples are provide in the form of environment variables. You could easily provide a configuration file [config.json](config.sample.json) instead of environment variables.

Where config values have with `<removed .. >` replace the text with your Authentication and API config.

### Microsoft Graph API v1.0

This example uses the `jsonpath paginator`. In this example, it requires a Microsoft Azure AD admin to register an APP to obtain an OAuth Token to perform an OAuth flow with the Microsoft Graph API. The details below may be different based on your setup, adjust accordingly.

Result: Two streamed datasets, one `whoami` a simple json response about yourself, two a sharepoint list `my_sharepoint_list`.

```
# Access MSOFFICE objects via the GraphAPI
export TAP_REST_API_MSDK_API_URL=https://graph.microsoft.com
export TAP_REST_API_MSDK_PAGINATION_REQUEST_STYLE="jsonpath_paginator"
export TAP_REST_API_MSDK_PAGINATION_RESPONSE_STYLE="hateoas_body"
export TAP_REST_API_MSDK_NEXT_PAGE_TOKEN_PATH="$['@odata.nextLink']"
export TAP_REST_API_MSDK_START_DATE="2001-01-01T00:00:00.00+12:00"
export TAP_REST_API_MSDK_AUTH_METHOD="oauth"
export TAP_REST_API_MSDK_USERNAME="<removed place in UPN/email address>"
export TAP_REST_API_MSDK_PASSWORD="<removed place in password>"
export TAP_REST_API_MSDK_GRANT_TYPE="password"
export TAP_REST_API_MSDK_ACCESS_TOKEN_URL="https://login.microsoftonline.com/<removed place in Azure AAD APP ID>/oauth2/v2.0/token"
export TAP_REST_API_MSDK_CLIENT_ID="<removed place in OAuth Client ID>"
export TAP_REST_API_MSDK_CLIENT_SECRET="<removed place in OAuth Client Secret>"
export TAP_REST_API_MSDK_SCOPE="<removed place in client scope url e.g. https://graph.microsoft.com/user.read>"
export TAP_REST_API_MSDK_STREAMS='[{"name": "whoami", "path": "/v1.0/me", "primary_keys": ["id"]},{"name": "my_sharepoint_list", "path": "/v1.0/sites/<removed place in SharePoint Site ID>/Lists/<removed place in SharePoint list id>/items/?expand=columns,items(expand=fields)", "primary_keys": ["id"], "records_path": "$.value[*].fields"}]'
```

### Gitlab API

This example uses the `simple header paginator` and returns 50 records from the Gitlab API for Projects. Note: There is an exception raised due to the 50 record limit - this is an example hence the limit.

```
# Access Gitlab projects via the GitLab API
export TAP_REST_API_MSDK_API_URL=https://gitlab.com/api/v4/projects
export TAP_REST_API_MSDK_PAGINATION_REQUEST_STYLE="simple_header_paginator"
export TAP_REST_API_MSDK_PAGINATION_RESULTS_LIMIT=50
export TAP_REST_API_MSDK_STREAMS='[{"name": "gitlab_projects", "primary_keys": ["id"]}]'
```

You could authenticate to Gitlab using a Personal Access Token (PAT) by adding this config.
```
export TAP_REST_API_MSDK_HEADERS='{"Authorization": "Bearer <removed PAT bearer token>"}'
```

### GitHub API

This example uses the `headerlink paginator` and returns approximately 250 records from the GitHub API for Projects.

```
# Access GitHub users via the GitHub API
export TAP_REST_API_MSDK_API_URL=https://api.github.com/users
export TAP_REST_API_MSDK_PAGINATION_REQUEST_STYLE="restapi_header_link_paginator"
export TAP_REST_API_MSDK_PAGINATION_RESPONSE_STYLE="header_link"
export TAP_REST_API_MSDK_PAGINATION_PAGE_SIZE=50
export TAP_REST_API_MSDK_PAGINATION_RESULTS_LIMIT=250
export TAP_REST_API_MSDK_STREAMS='[{"name": "github_users", "primary_keys": ["id"]}]'
```

You could authenticate to GitHub using a Personal Access Token (PAT) by adding this config.
```
export TAP_REST_API_MSDK_HEADERS='{"Authorization": "Bearer <removed PAT bearer token>"}'
```

### FHIR API

This example uses the `jsonpath paginator` to access a FHIR API. It uses the `hateoas response style` to process the next tokens.

This particular configuration will do an intial load of all data for a given resource defined in the `streams` config from the 01-Jan-2001. It will in subsequent runs incrementally pull changed data based on the lastUpdated timestamp by searching for records greater than the highest last updated timestamp. In this example the PlanDefinition FHIR resource is being extracted.

You will need appropriate OAuth Token details provided by the Administrator of the API.

```
export TAP_REST_API_MSDK_API_URL=<remove put in the FHIR API url>
export TAP_REST_API_MSDK_PAGINATION_REQUEST_STYLE="jsonpath_paginator"
export TAP_REST_API_MSDK_PAGINATION_RESPONSE_STYLE="hateoas_body"
export TAP_REST_API_MSDK_NEXT_PAGE_TOKEN_PATH="$.link[?(@.relation=='next')].url"
export TAP_REST_API_MSDK_START_DATE="2001-01-01T00:00:00.00+12:00"
export TAP_REST_API_MSDK_AUTH_METHOD="oauth"
export TAP_REST_API_MSDK_GRANT_TYPE="client_credentials"
export TAP_REST_API_MSDK_ACCESS_TOKEN_URL="https://login.microsoftonline.com/<removed place in Azure AAD APP ID>/oauth2/v2.0/token"
export TAP_REST_API_MSDK_CLIENT_ID="<removed place in OAuth Client ID>"
export TAP_REST_API_MSDK_CLIENT_SECRET="<removed place in OAuth Client Secret>"
export TAP_REST_API_MSDK_SCOPE="<removed place in client scope url>"
export TAP_REST_API_MSDK_STREAMS='[{"name":"plan_definition","path":"/PlanDefinition","primary_keys":["id"],"records_path":"$.entry[*].resource","replication_key":"meta_lastUpdated","search_parameter":"_lastUpdated","source_search_query": "gt$last_run_date"}]'
```

### NOAA API Example

This example uses the `offset paginator` to access the NOAA API to return location categories. In this example the offset tokens are not in the default location of `pagination` so the `next_page_token_path` is set to the NOAA API offset location in the json response i.e. `'$.metadata.resultset'`. This example also sets a limit parameter in the `streams` to only return 5 records at a time to prove the pagination is working.

```
# Access Locations Categories objects via the NOAA API
export TAP_REST_API_MSDK_API_URL=https://www.ncei.noaa.gov/cdo-web/api/v2
export TAP_REST_API_MSDK_HEADERS='{"token": "<enter NOAA token>"}'
export TAP_REST_API_MSDK_NEXT_PAGE_TOKEN_PATH='$.metadata.resultset'
export TAP_REST_API_MSDK_PAGINATION_REQUEST_STYLE="offset_paginator"
export TAP_REST_API_MSDK_PAGINATION_RESPONSE_STYLE="style1"
export TAP_REST_API_MSDK_PAGINATION_TOTAL_LIMIT_PARAM="count"
export TAP_REST_API_MSDK_STREAMS='[{"name": "locationcategories", "params": {"limit": "5"}, "path": "/locationcategories", "primary_keys": ["id"], "records_path": "$.results[*]"}]'
```

### dbt Cloud API Example

This example uses the `offset paginator` to access the dbt Cloud API to return location categories. In this example the offset tokens are not in the default location of `pagination` so the `next_page_token_path` is set to the dbt API offset location in the json response i.e. `'$.extra'`. This example also sets the streams record_path to `"$.data[*]"` which is the location of the data.

```
# Access Locations Categories objects via the dbt Cloud API
# Access Gitlab objects via the dbt Cloud API
export TAP_REST_API_MSDK_API_URL=https://<removed your url>.getdbt.com/api/v2/accounts/<removed account id>
export TAP_REST_API_MSDK_HEADERS='{"Authorization": "Bearer <removed place in bearer token>"}'
export TAP_REST_API_MSDK_NEXT_PAGE_TOKEN_PATH='$.extra'
export TAP_REST_API_MSDK_PAGINATION_REQUEST_STYLE="offset_paginator"
export TAP_REST_API_MSDK_PAGINATION_RESPONSE_STYLE="style1"
export TAP_REST_API_MSDK_PAGINATION_TOTAL_LIMIT_PARAM="total_count"
export TAP_REST_API_MSDK_STREAMS='[{"name": "jobs", "path": "/jobs", "primary_keys": ["id"], "records_path": "$.data[*]"}]'
```

### AWS OpenSearch API Example

This complex example uses the [AWS4Auth](https://github.com/tedder/requests-aws4auth) authenticator to provide signed AWS credentials in the requests to the AWS OpenSearch API endpoint. The `auth_method` is set to 'aws', and the required `aws_credentials` are provided.

Note: The AWS authentication does support [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html) environment variables and aws_profiles.

For pagination, the next page token is located in the **last** returned record. Using JSON Path the appropriate token response can be extracted via '$.hits.hits[-1:].sort' (-1 selects the last record in the array) and this is set in the `next_page_token_path` config setting. The API parameter used to select the next page is 'search_after' and this is set in the `pagination_next_page_param` config setting. To enable pagination in OpenSearch an API parameter named 'sort' must be set to a unique key e.g. '_id'. The number of records to returned per page is controlled via an API parameter called 'size'.

The OpenSearch API has a complex incremental replication query which must be sent in the request body. This is enabled by setting the `use_request_body_not_params` to True.

Finally set the replication suite of config settings ( `start_date`, `replication_key`, `source_search_field`, and `source_search_query` ) to enable incremental replication of data since the last run. For OpenSearch there is a complex query template which must be set in the streams `source_search_query` config setting.

Unlike most API requests, the API query is against an API parameter named `query` rather than the name of the API field. For this reason the `source_search_field` is set to 'query' in the streams array. Additionally, the streams record_path to `"$.hits.hits[*]"` which is the location of the records in the requests response.

```
# Access AWS objects via the AWS Open/Elastic Search API
export TAP_REST_API_MSDK_API_URL="https://<endpoint>.<aws region>.<aws service>.amazonaws.com"
export TAP_REST_API_MSDK_AWS_CREDENTIALS='{"aws_access_key_id": "<removed aws access key id>", "aws_secret_access_key": "removed aws secret access key>", "aws_region": "<aws region e.g. us‑east‑1>", "aws_service": "<aws service e.g. es for opensearch>", "create_signed_credentials": true}'
export TAP_REST_API_MSDK_START_DATE="2001-01-01T00:00:00.00+12:00"
export TAP_REST_API_MSDK_PAGINATION_REQUEST_STYLE="jsonpath_paginator"
export TAP_REST_API_MSDK_PAGINATION_RESPONSE_STYLE="offset"
export TAP_REST_API_MSDK_USE_REQUEST_BODY_NOT_PARAMS=true
export TAP_REST_API_MSDK_NEXT_PAGE_TOKEN_PATH='$.hits.hits[-1:].sort'
export TAP_REST_API_MSDK_PAGINATION_NEXT_PAGE_PARAM="search_after"
export TAP_REST_API_MSDK_AUTH_METHOD='aws'
export TAP_REST_API_MSDK_STREAMS='[{"name": "careplan", "params": {"size": 100, "sort": "_id"}, "path": "/careplan/_search", "primary_keys": [], "records_path": "$.hits.hits[*]", "replication_key": "_source_meta_lastUpdated", "source_search_field": "query", "source_search_query": "{\"bool\": {\"filter\": [{\"range\": { \"meta.lastUpdated\": { \"gt\": \"$last_run_date\" }}}] }}"}]'
```

## Usage


You can easily run `tap-rest-api-msdk` by itself or in a pipeline using [Meltano](www.meltano.com).

### Executing the Tap Directly

```bash
tap-rest-api-msdk --version
tap-rest-api-msdk --help
tap-rest-api-msdk --config CONFIG --discover > ./catalog.json
```

or

```bash
bash tap-rest-api-msdk --config=config.sample.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests/` directory and
then run:

```bash
poetry run pytest
```

You can also test the `tap-rest-api-msdk` CLI interface directly using `poetry run`:

```bash
poetry run tap-rest-api-msdk --help
```

### Continuous Integration
Run through the full suite of tests and linters by running

```bash
poetry run tox -e py
```

These must pass in order for PR's to be merged.

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

This project comes with an example `meltano.yml` project file already created.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-rest-api-msdk
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-rest-api-msdk --version
# OR run a test `elt` pipeline:
meltano elt tap-rest-api-msdk target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
