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
        - name: auth_method
        - name: auth_token
        - name: name
        - name: path
        - name: params
        - name: headers
        - name: records_path
        - name: primary_keys
        - name: replication_key
        - name: except_keys
        - name: num_inference_records
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

Config Options:
- `api_url`: the base url/endpoint for the desired api
- `name`: required: name of the stream.
- `path`: optional: the path appeneded to the `api_url`.
- `params`: optional: an object of objects that provide the `params` in a `requests.get` method.
- `records_path`: optional: a jsonpath string representing the path in the requests response that contains the records to process. Defaults to `$[*]`.
- `primary_keys`: required: a list of the json keys of the primary key for the stream.
- `replication_key`: optional: the json key of the replication key. Note that this should be an incrementing integer or datetime object.
- `except_keys`: This tap automatically flattens the entire json structure and builds keys based on the corresponding paths.
  Keys, whether composite or otherwise, listed in this dictionary will not be recursively flattened, but instead their values will be
  turned into a json string and processed in that format. This is also automatically done for any lists within the records; therefore,
  records are not duplicated for each item in lists.
- `num_inference_keys`: optional: number of records used to infer the stream's schema. Defaults to 50

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

Create tests within the `tap_rest_api_msdk/tests` subfolder and
then run:

```bash
poetry run pytest
```

You can also test the `tap-rest-api-msdk` CLI interface directly using `poetry run`:

```bash
poetry run tap-rest-api-msdk --help
```

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
