# tap-rest-api-msdk

`tap-rest-api-msdk` is a Singer tap for generic rest-apis. The novelty of this particular tap
is that it will auto-discover the stream's schema.

This is particularly useful if you have a stream with a very large and complex schema or
a stream that outputs records with varying schemas for each record. Can also be used for
simpler more reliable streams.

Please note that authentication capabilities have not yet been developed for this tap,
unless you are able to pass the authentication through the header.
If you are interested in contributing this, please fork and make a pull request.

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
        - name: name
        - name: path
        - name: params
        - name: headers
        - name: records_path
        - name: next_page_token_path
        - name: pagination_request_style
        - name: pagination_response_style
        - name: pagination_page_size
        - name: primary_keys
        - name: replication_key
        - name: except_keys
        - name: num_inference_records
        - name: pagination_request_style
        - name: pagination_response_style
        - name: pagination_page_size
        - name: next_page_token_path
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
- `pagination_request_style`: optional: style for requesting pagination, defaults to `default`, see Pagination below.
- `pagination_response_style`: optional: style of pagination results, defaults to `default`, see Pagination below.
- `pagination_page_size`: optional: limit for size of page, defaults to None.
- `next_page_token_path`: optional: a jsonpath string representing the path to the "next page" token. Defaults to `$.next_page`.
- `streams`: required: a list of objects that contain the configuration of each stream. See stream-level params below.
- `path`: optional: see stream-level params below.
- `params`: optional: see stream-level params below.
- `headers`: optional: see stream-level params below.
- `records_path`: optional: see stream-level params below.
- `primary_keys`: optional: see stream-level params below.
- `replication_key`: optional: see stream-level params below.
- `except_keys`: optional: see stream-level params below.
- `num_inference_keys`: optional:  see stream-level params below.

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
- `scheam`: optional: A valid Singer schema or a path-like string that provides
  the path to a `.json` file that contains a valid Singer schema. If provided, 
  the schema will not be inferred from the results of an api call.

## Pagination
Pagination is a complex topic as there is no real single standard, and many different implementations.  Unless options are provided, both the request and results stype default to the `default`, which is the pagination style originally implemented.

### Default Request Style
The default request style for pagination is described below:
- Use next_page_token_path if provided to extract the token from response if found; otherwise
- use X-Next-Page header from response

### Default Response Style
The default response style for pagination is described below:
- If there is a token, add that as a `page` URL parameter.

### Additional Request Styles
There are additional request styles supported as follows.
- `style1` - This style uses URL parameters named offset and limit
  - `offset` is calculated from the previous response, or not set if there is no previous response
  - `limit` is set to the `pagination_page_size` value, if specified, or not set

### Additional Response Styles
There are additional response styles supported as follows.
- `style1` - This style retrieves pagination information from the `pagination` top-level element in the response.  Expected format is as follows:
    ```json
    "pagination": {
        "total": 136,
        "limit": 2,
        "offset": 2
    }
    ```
  The next page token, which in this case is really the next starting record number, is calculated by the limit, current offset, or None is returned to indicate no more data.  For this style, the response style _must_ include the limit in the response, even if none is specified in the request, as well as total and offset to calculate the next token value.

## Usage

You can easily run `tap-rest-api-msdk` by itself or in a pipeline using [Meltano](www.meltano.com).

### Executing the Tap Directly

```bash
tap-rest-api-msdk --version
tap-rest-api-msdk --help
tap-rest-api-msdk --config CONFIG --discover > ./catalog.json
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
