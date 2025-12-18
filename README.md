# target-s3-jsonl

[![GitHub - License](https://img.shields.io/github/license/ome9ax/target-s3-jsonl?color=blue)](https://github.com/ome9ax/target-s3-jsonl/blob/main/LICENSE)
[![Python package builder](https://github.com/ome9ax/target-s3-jsonl/workflows/Python%20package/badge.svg)](https://github.com/ome9ax/target-s3-jsonl/actions)
[![codecov](https://codecov.io/gh/ome9ax/target-s3-jsonl/branch/main/graph/badge.svg?token=KV0cn4jKs2)](https://codecov.io/gh/ome9ax/target-s3-jsonl)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/target-s3-jsonl.svg)](https://pypi.org/project/target-s3-jsonl)
[![PyPI version](https://badge.fury.io/py/target-s3-jsonl.svg)](https://badge.fury.io/py/target-s3-jsonl)
[![PyPi project installs](https://img.shields.io/pypi/dm/target-s3-jsonl.svg?maxAge=2592000&label=installs&color=%2327B1FF)](https://pypistats.org/packages/target-s3-jsonl)

[Singer](https://www.singer.io/) target that uploads loads data to S3 in JSONL format
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

`target-s3-jsonl` is a [Singer](https://singer.io) Target which intend to work with regular [Singer](https://singer.io) Tap. It take the output of the tap and export it as a [JSON Lines](http://jsonlines.org/) files into an AWS S3 bucket.

This package is built using the [`target-core`](https://gitlab.com/singer-core/target-core) library.

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

### Defaults
**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.

```bash
python -m venv ~/.virtualenvs/target-s3-jsonl
~/.virtualenvs/target-s3-jsonl/bin/pip install target-s3-jsonl
```

### Head
```bash
python -m venv ~/.virtualenvs/target-s3-jsonl
~/.virtualenvs/target-s3-jsonl/bin/pip install --upgrade git+https://github.com/ome9ax/target-s3-jsonl.git@main
```

### Alternative
```bash
python -m venv ~/.virtualenvs/target-s3-jsonl
source ~/.virtualenvs/target-s3-jsonl/bin/activate
pip install target-s3-jsonl
deactivate
```

## Usage

Like any other target that's following the singer specificiation:

`some-singer-tap | target-s3-jsonl --config [config.json]`

It's reading incoming messages from STDIN and using the properites in `config.json` to upload data into AWS S3.

## Configuration settings

Running the the target connector requires a `config.json` file. An example with the minimal settings:

```json
{
    "s3_bucket": "my_bucket"
}
```

### Profile based authentication

Profile based authentication used by default using the `default` profile. To use another profile set `aws_profile` parameter in `config.json` or set the `AWS_PROFILE` environment variable.

### Non-Profile based authentication

For non-profile based authentication set `aws_access_key_id` , `aws_secret_access_key` and optionally the `aws_session_token` parameter in the `config.json`. Alternatively you can define them out of `config.json` by setting `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` environment variables.

### Full list of options in `config.json`:

#### Inherited from `target-core`

| Property                            | Type    | Mandatory? | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| path_template                   | String  |            | (Default: None) Custom naming convention of the s3 key. Replaces tokens `stream`, and `date_time` with the appropriate values.<br><br>Supports datetime and other python advanced string formatting e.g. `{stream}_{date_time:%FT%T.%f}.jsonl` or `{stream:_>8}/{date_time:%Y}/{date_time:%m}/{date_time:%d}/{date_time:%Y%m%d_%H%M%S_%f}.json`.<br><br>Supports "folders" in s3 keys e.g. `my_folder/my_sub_folder/{stream}/export_date={date}/{date_time}.json`. |
| memory_buffer                       | Integer |            | Memory buffer's size used for non partitioned files before storing the data into the temporary file. 64Mb used by default if unspecified. |
| file_size                           | Integer |            | File partitinoning by `size_limit`. File parts will be created. The `path_template` must contain a part section for the part number. Example `"path_template": "{stream}_{date_time:%Y%m%d_%H%M%S}_part_{part:0>3}.json"`. |
| compression                         | String  |            | The type of compression to apply before uploading. Supported options are `none` (default), `gzip`, and `lzma`. For gzipped files, the file extension will automatically be changed to `.json.gz` for all files. For `lzma` compression, the file extension will automatically be changed to `.json.xz` for all files. |
| timezone_offset                     | Integer |            | Offset value in hour. Use offset `0` hours is you want the `path_template` to use `utc` time zone. The `null` values is used by default. |
| work_dir                            | String  |            | (Default: platform-dependent) Directory for temporary JSONL files with RECORD messages. |

#### Specific For `target-s3-jsonl`

| Property                            | Type    | Mandatory? | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| aws_profile                         | String  |            | AWS profile name for profile based authentication. If not provided, `AWS_PROFILE` environment variable will be used. |
| aws_endpoint_url                    | String  |            | AWS endpoint URL. |
| aws_access_key_id                   | String  |            | S3 Access Key Id. If not provided, `AWS_ACCESS_KEY_ID` environment variable will be used. |
| aws_secret_access_key               | String  |            | S3 Secret Access Key. If not provided, `AWS_SECRET_ACCESS_KEY` environment variable will be used. |
| aws_session_token                   | String  |            | AWS Session token. If not provided, `AWS_SESSION_TOKEN` environment variable will be used. |
| encryption_type                     | String  |            | (Default: 'none') The type of encryption to use. Current supported options are: 'none' and 'KMS'. |
| encryption_key                      | String  |            | A reference to the encryption key to use for data encryption. For KMS encryption, this should be the name of the KMS encryption key ID (e.g. '1234abcd-1234-1234-1234-1234abcd1234'). This field is ignored if 'encryption_type' is none or blank. |
| role_arn                            | String  |            | The ARN of the role to assume |

## Cleanup Script

The repository includes a standalone utility script `cleanup_s3_files.py` that can be used to clean up empty JSONL files from S3. This is useful for removing files that contain only schema information but no actual data records.

### Features
- Scans S3 for JSONL files and identifies those without RECORD entries
- Supports dry-run mode to preview which files would be deleted
- Can filter by specific prefixes within date directories
- Batch deletion for efficient processing
- Can delete files from a pre-generated list
- Automatic retry logic with exponential backoff for AWS errors

### Usage

#### Scan and delete empty files:
```bash
python cleanup_s3_files.py --bucket my-bucket --prefix output/2025/09/ --dry-run
```

#### Filter to specific prefixes within each day:
```bash
python cleanup_s3_files.py --bucket my-bucket --prefix output/2025/09/ \
  --additional-prefixes "prefix1,prefix2"
```

#### Save dry-run results to a file:
```bash
python cleanup_s3_files.py --bucket my-bucket --prefix output/2025/09/ \
  --dry-run --dry-run-output files_to_delete.txt
```

#### Delete files from a list:
```bash
python cleanup_s3_files.py --delete-from-file files_to_delete.txt
```

### Options
- `--bucket`: S3 bucket name (required unless using --delete-from-file)
- `--prefix`: S3 prefix to search under (e.g., "output/2025/09/")
- `--additional-prefixes`: Comma-separated list of prefixes to filter within each day
- `--dry-run`: Preview what would be deleted without actually deleting
- `--dry-run-output`: File path to save dry-run results
- `--delete-from-file`: Delete S3 objects listed in this file
- `--debug`: Enable debug logging

## Test
### Install the tools
```bash
pip install tox
```

### Run pytest
```bash
tox -e py
```

### Lint & Static typing validation
```bash
tox -e lint,static
```

## Release
1. Update the version number at the beginning of `target-s3-jsonl/target_s3_json/__init__.py`
2. Merge the changes PR into `main`
3. Release the new version in github

## License

Apache License Version 2.0
