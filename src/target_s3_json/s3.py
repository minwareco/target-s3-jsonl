from io import BufferedIOBase
import sys
from os import environ
from functools import partial
from pathlib import Path
import argparse
import json
import gzip
import lzma
import time
from typing import Callable, Dict, Any, List, TextIO
from asyncio import sleep, to_thread
from concurrent.futures import ThreadPoolExecutor, Future

import backoff
from boto3.session import Session
from botocore.exceptions import ClientError
from botocore.client import BaseClient
from botocore.client import Config

from .stream import Loader
from .file import config_file, save_json, config_compression

from target._logger import get_logger
LOGGER = get_logger()


def _log_backoff_attempt(details: Dict) -> None:
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


def _retry_pattern() -> Callable:
    return backoff.on_exception(
        backoff.expo,
        ClientError,
        max_tries=5,
        on_backoff=_log_backoff_attempt,
        factor=10)


def config_compression(config_default: Dict) -> Dict:
    config: Dict[str, Any] = {
        'compression': 'none'
    } | config_default

    if f"{config.get('compression')}".lower() == 'gzip':
        config['open_func'] = gzip.compress
        config['path_template'] = config['path_template'] + '.gz'

    elif f"{config.get('compression')}".lower() == 'lzma':
        config['open_func'] = lzma.compress
        config['path_template'] = config['path_template'] + '.xz'

    elif f"{config.get('compression')}".lower() in {'', 'none'}:
        config['open_func'] = open

    else:
        raise NotImplementedError(
            "Compression type '{}' is not supported. "
            "Expected: 'none', 'gzip', or 'lzma'"
            .format(f"{config.get('compression')}".lower()))

    return config


@_retry_pattern()
def create_session(config: Dict) -> Session:
    # NOTE: Get the required parameters from config file and/or environment variables
    aws_access_key_id = config.get('aws_access_key_id') or environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('aws_secret_access_key') or environ.get('AWS_SECRET_ACCESS_KEY')
    aws_session_token = config.get('aws_session_token') or environ.get('AWS_SESSION_TOKEN')
    aws_profile = config.get('aws_profile') or environ.get('AWS_PROFILE')
    aws_endpoint_url = config.get('aws_endpoint_url')
    role_arn = config.get('role_arn')

    endpoint_params = {'endpoint_url': aws_endpoint_url} if aws_endpoint_url else {}

    # NOTE: AWS credentials based authentication
    if aws_access_key_id and aws_secret_access_key:
        aws_session: Session = Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token)
    # NOTE: AWS Profile based authentication
    else:
        aws_session = Session(profile_name=aws_profile)

    # NOTE: AWS credentials based authentication assuming specific IAM role
    if role_arn:
        role_name = role_arn.split('/', 1)[1]
        sts: BaseClient = aws_session.client('sts', **endpoint_params)
        resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=f'role-name={role_name}-profile={aws_profile}')
        credentials = {
            'aws_access_key_id': resp['Credentials']['AccessKeyId'],
            'aws_secret_access_key': resp['Credentials']['SecretAccessKey'],
            'aws_session_token': resp['Credentials']['SessionToken'],
        }
        aws_session = Session(**credentials)
        LOGGER.info(f'Creating s3 session with role {role_name}')

    return aws_session


def get_encryption_args(config: Dict[str, Any]) -> tuple:
    if config.get('encryption_type', 'none').lower() == "none":
        # NOTE: No encryption config (defaults to settings on the bucket):
        encryption_desc: str = ''
        encryption_args: dict = {}
    elif config.get('encryption_type', 'none').lower() == 'kms':
        if config.get('encryption_key'):
            encryption_desc = " using KMS encryption key ID '{}'".format(config.get('encryption_key'))
            encryption_args = {'ExtraArgs': {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': config.get('encryption_key')}}
        else:
            encryption_desc = ' using default KMS encryption'
            encryption_args = {'ExtraArgs': {'ServerSideEncryption': 'aws:kms'}}
    else:
        raise NotImplementedError(
            "Encryption type '{}' is not supported. "
            "Expected: 'none' or 'KMS'"
            .format(config.get('encryption_type')))
    return encryption_desc, encryption_args


@_retry_pattern()
def put_object(config: Dict[str, Any], file_metadata: Dict, stream_data: List) -> None:
    encryption_desc, encryption_args = get_encryption_args(config)

    config['client'].put_object(
        Body=config['open_func'](  # NOTE: stream compression with gzip.compress, lzma.compress
            b''.join(json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in stream_data)),
        Bucket=config.get('s3_bucket'),
        Key=file_metadata['relative_path'],
        **encryption_args.get('ExtraArgs', {}))

    LOGGER.info("%s uploaded to bucket %s at %s%s",
                file_metadata['absolute_path'].as_posix(), config.get('s3_bucket'), file_metadata['relative_path'], encryption_desc)


@_retry_pattern()
def upload_file(config: Dict[str, Any], file_metadata: Dict) -> None:
    if not config.get('local', False) and (file_metadata['absolute_path'].stat().st_size if file_metadata['absolute_path'].exists() else 0) > 0:
        encryption_desc, encryption_args = get_encryption_args(config)

        config['client'].upload_file(
            file_metadata['absolute_path'].as_posix(),
            config.get('s3_bucket'),
            file_metadata['relative_path'],
            **encryption_args)

        LOGGER.info('%s uploaded to bucket %s at %s%s',
                    file_metadata['absolute_path'].as_posix(), config.get('s3_bucket'), file_metadata['relative_path'], encryption_desc)

        if config.get('remove_file', True):
            # NOTE: Remove the local file(s)
            file_metadata['absolute_path'].unlink()  # missing_ok=False


async def upload_thread(config: Dict[str, Any], file_metadata: Dict) -> Future:

    return await to_thread(
        *([config['executor'].submit] if config.get('thread_pool', True) else []),
        upload_file,
        config,
        file_metadata)


def config_s3(config_default: Dict[str, Any], datetime_format: Dict[str, str] = {
        'date_time_format': ':%Y%m%dT%H%M%S',
        'date_format': ':%Y%m%d'}) -> Dict[str, Any]:
    # NOTE: to_snake = lambda s: '_'.join(findall(r'[A-Z]?[a-z]+|\d+|[A-Z]{1,}(?=[A-Z][a-z]|\W|\d|$)', line)).lower()

    if 'temp_dir' in config_default:
        LOGGER.warning('`temp_dir` configuration option is deprecated and support will be removed in the future, use `work_dir` instead.')
        config_default['work_dir'] = config_default.pop('temp_dir')

    if 'naming_convention' in config_default:
        LOGGER.warning(
            '`naming_convention` configuration option is deprecated and support will be removed in the future, use `path_template` instead.'
            ', `{timestamp}` key pattern is now replaced by `{date_time}`'
            ', and `{date}` key pattern is now replaced by `{date_time:%Y%m%d}`')
        config_default['path_template'] = config_default.pop('naming_convention') \
            .replace('{timestamp:', '{date_time:').replace('{date:', '{date_time:') \
            .replace('{timestamp}', '{date_time%s}' % datetime_format['date_time_format']) \
            .replace('{date}', '{date_time%s}' % datetime_format['date_format'])

    missing_params = {'s3_bucket'} - set(config_default.keys())
    if missing_params:
        raise Exception(f'Config is missing required settings: {missing_params}')

    return config_default

curSchemaBuffer = b''
lastFlushTime = time.time()

class WrappedIoBuffer():
    def __init__(self, input: BufferedIOBase, flushSeconds) -> None:
        self.input = input
        self.closed = False
        self.empty = False
        self.buffer = curSchemaBuffer
        self.storedLine = False
        self.stoppedState = False
        self.flushSeconds = flushSeconds

    def readable(self):
        return not self.closed

    def writable(self):
        return False

    def seekable(self):
        return False

    def readMore(self):
        global lastFlushTime

        if self.empty or self.closed:
            return
        readData = self.input.readline()
        if not readData:
            self.empty = True
            return

        try:
            line = json.loads(readData)
        except json.decoder.JSONDecodeError:
            LOGGER.error(f'Unable to parse:\n{readData}')
            raise

        # Don't read any more after the state if we want to let it flush
        if line['type'] == 'STATE':
            curTime = time.time()
            if curTime - lastFlushTime > self.flushSeconds:
                self.empty = True
                self.stoppedState = True
                lastFlushTime = curTime
        # Save schemas becuase they have to be output after each state
        if line['type'] == 'SCHEMA':
            global curSchemaBuffer
            curSchemaBuffer += readData

        self.buffer += readData

    def read(self, size):
        readSize = 8192 if size == -1 else size
        if len(self.buffer) < readSize:
            self.readMore()
        if len(self.buffer) == 0:
            self.closed = True
            return b''
        readData = self.buffer[:readSize]
        self.buffer = self.buffer[readSize:]
        return readData

class WrappedTextIO():
    def __init__(self, input: TextIO, flushSeconds) -> None:
        self.input = input
        self.hasMore = True
        self.buffer = WrappedIoBuffer(input.buffer, flushSeconds)

    def stoppedState(self):
        return self.buffer.stoppedState


def main(lines: TextIO = sys.stdin) -> None:
    '''Main'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()
    lastTime = 0
    save_s3: Callable = partial(save_json, post_processing=upload_thread)
    curConfig = config_s3(json.loads(Path(args.config).read_text(encoding='utf-8')))
    client: BaseClient = None
    
    while True:
        # Make sure file names don't collide
        curTime = round(time.time())
        if curTime == lastTime:
            sleep(1)
        lastTime = curTime
        config = config_compression(config_file(curConfig))
        proxy_config = {}
        if config.get('proxies'):
            proxy_config = config.get('proxies')
        if environ.get('HTTP_PROXY', None):
            proxy_config = {
                'http': environ.get('HTTP_PROXY'),
                'https': environ.get('HTTPS_PROXY')
            }

        curLines = WrappedTextIO(lines, config.get('flush_seconds') if config.get('flush_seconds') else 10*60)
        if not client:
            client = create_session(config).client('s3',
                                                   **({'endpoint_url': config.get('aws_endpoint_url')} if config.get('aws_endpoint_url') else {}),
                                                   config=Config(proxies=proxy_config))
        with ThreadPoolExecutor() as executor:
            Loader(config | {'client': client, 'executor': executor, 'add_metadata_columns': True }, writeline=save_s3).run(curLines)
        if not curLines.stoppedState():
            break



# from pyarrow import parquet
# from pyarrow.json import read_json
# import pandas
# def write_parquet(config: Dict[str, Any], file_meta: Dict, file_data: List) -> None:
#     s3_fs: S3FileSystem = S3FileSystem(anon=False, s3_additional_kwargs={'ACL': 'bucket-owner-full-control'}, asynchronous=False)

#     # NOTE: Create parquet file using Pandas
#     pandas.json_normalize(file_data).to_parquet(path = file_meta['absolute_path'], filesystem = s3_fs)
#     # NOTE: Synchronous Alternative without df middle step, read_json not yet as efficient as pandas. Worth keeping on check.
#     with BytesIO(b''.join(json.dumps(record, ensure_ascii=False).encode('utf-8') + b'\n' for record in file_data)) as data:
#         parquet.write_table(read_json(data), file_meta['relative_path'], filesystem=s3_fs)


# NOTE: https://github.com/aws/aws-cli/issues/3784
# async def search(client: BaseClient, bucket: str, prefix: str, regex_path: str) -> Generator:
#     '''
#     perform a flat listing of the files within a bucket
#     '''
#     regex_pattern: Pattern = compile(regex_path)

#     paginator = client.get_paginator('list_objects_v2')
#     files_metadata = paginator.paginate(Bucket=bucket, Prefix=prefix)
#     for file_path in map(lambda x: x.get('Key', ''), await to_thread(files_metadata.search, 'Contents')):
#         if match(regex_pattern, file_path):
#             yield file_path


# async def sync(
#     start_date: datetime = eval(config.get('date_time', 'datetime.now().astimezone(timezone.utc)'))
#     client: BaseClient, semaphore: Semaphore, source_bucket: str, source_key: str, target_bucket: str, target_key: str, overwrite: bool = False) -> None:
#     await gather(*[shield(search(client, semaphore, source_bucket, source_key, target_bucket, target_root + source_key.removeprefix(source_root), overwrite))
#         async for source_key in search(client, source_bucket, source_root, source_regexp)])
#     async with semaphore:
#         if not overwrite and 'Contents' in client.list_objects_v2(Bucket=target_bucket, Prefix=target_key, MaxKeys=1):
#             LOGGER.debug(f'S3 Bucket Sync - "s3://{target_bucket}/{target_key}" already exists.')
#         else:
#             await to_thread(client.copy, {'Bucket': source_bucket, 'Key': source_key}, target_bucket, target_key)
#             LOGGER.info(f'S3 Bucket Sync - "s3://{source_bucket}/{source_key}" to "s3://{target_bucket}/{target_key}" copy completed.')
