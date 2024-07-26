from typing import Callable, Dict, Any, List, Optional
import sys
from pathlib import Path
import datetime
import gzip
import lzma
import json
from tempfile import gettempdir
from uuid import uuid4
from asyncio import to_thread

# Package imports
from ._logger import get_logger

LOGGER = get_logger()


def config_file(config_default: Dict[str, Any] = {}, datetime_format: Dict = {
        'date_time_format': ''}) -> Dict:

    config: Dict[str, Any] = {
        'path_template': '{stream}-{date_time%(date_time_format)s}.json' % datetime_format,
        'memory_buffer': 64e6
    } | config_default

    config['path_template'] = config['path_template'] \
        .replace('{date_time}', '{date_time%(date_time_format)s}' % datetime_format)

    # NOTE: Use the system specific temp directory if no custom work_dir provided
    config['work_path'] = Path(config.get('work_dir', gettempdir())).expanduser()

    config['date_time'] = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=config.get('timezone_offset', 0)))) \
        if config.get('timezone_offset') is not None \
        else datetime.datetime.utcnow()

    if config.get('file_size'):
        config.pop('memory_buffer', None)

    return config


def config_compression(config_default: Dict[str, Any] = {}) -> Dict:
    config: Dict[str, Any] = {
        'compression': 'none'
    } | config_default

    if f"{config.get('compression')}".lower() == 'gzip':
        config['open_func'] = gzip.open
        config['path_template'] = config['path_template'] + '.gz'

    elif f"{config.get('compression')}".lower() == 'lzma':
        config['open_func'] = lzma.open
        config['path_template'] = config['path_template'] + '.xz'

    elif f"{config.get('compression')}".lower() in {'', 'none'}:
        config['open_func'] = open

    else:
        raise NotImplementedError(
            "Compression type '{}' is not supported. "
            "Expected: 'none', 'gzip', or 'lzma'"
            .format(f"{config.get('compression')}".lower()))

    return config


def _get_relative_path(stream: str, config: Dict[str, Any], date_time: datetime.datetime, part: int = 1) -> str:
    '''Creates and returns an S3 key for the stream

    Parameters
    ----------
    stream : str
        incoming stream name that is written in the file
    config : dict
        configuration dictionary
    date_time : datetime
        Date used in the path template

    Returns
    -------
    out : ``str``
        The formatted path.
    '''

    # NOTE: Replace dynamic tokens
    key = config['path_template'].format(stream=stream, date_time=date_time, part=part, uuid=uuid4())

    prefix = config.get('key_prefix', '')
    return str(Path(key).with_name(prefix + Path(key).name)) if prefix else key


def set_schema(stream: str, config: Dict, stream_data: Dict, schema: Dict = {}) -> None:
    # NOTE: get the file key. Persistent array data storage.
    if stream not in stream_data:
        relative_path = _get_relative_path(stream=stream, config=config, date_time=config['date_time'], part=1)
        stream_data[stream] = {
            'part': 1,
            'path': {1: {'relative_path': relative_path,
                         'absolute_path': config['work_path'] / relative_path}},
            'file_data': []}

        for path in stream_data[stream]['path'].values():
            path['absolute_path'].unlink(missing_ok=True)
            # path['absolute_path'].parent.mkdir(parents=True, exist_ok=True)


async def write(config: Dict[str, Any], file_metadata: Dict, stream_data: List) -> None:
    if any(stream_data):
        file_metadata['absolute_path'].parent.mkdir(parents=True, exist_ok=True)

        with config['open_func'](file_metadata['absolute_path'], 'at', encoding='utf-8') as output_file:
            await to_thread(output_file.writelines, (json.dumps(record, ensure_ascii=False, default=str) + '\n' for record in stream_data))

        del stream_data[:]


async def save_json(
    stream: str, stream_data: Dict, config: Dict[str, Any], record: Optional[Dict[Any, Any]] = None,
    save: Callable = write, post_processing: Optional[Callable] = None
) -> None:

    # NOTE: If file partition limit
    if record is not None and config.get('file_size') is not None:
        # NOTE: If over size
        if (stream_data[stream]['path'][stream_data[stream]['part']]['absolute_path'].stat().st_size
                if stream_data[stream]['path'][stream_data[stream]['part']]['absolute_path'].exists() else 0) >= config.get('file_size'):
            # TODO: post processing
            if post_processing:
                LOGGER.info('******post processing')
                await post_processing(config, stream_data[stream]['path'][stream_data[stream]['part']])
            LOGGER.debug("File '%s' saved using open_func '%s'",
                         stream_data[stream]['path'][stream_data[stream]['part']]['absolute_path'], config['open_func'].__name__)

            stream_data[stream]['part'] += 1
            relative_path: str = _get_relative_path(stream=stream, config=config, date_time=config['date_time'], part=stream_data[stream]['part'])
            stream_data[stream]['path'] |= {
                stream_data[stream]['part']: {
                    'relative_path': relative_path,
                    'absolute_path': config['work_path'] / relative_path}}

        await save(config, stream_data[stream]['path'][stream_data[stream]['part']], [record])

    # NOTE: If memory buffer limit
    elif record is not None:
        # NOTE: If over size
        if sys.getsizeof(stream_data[stream]['file_data']) >= config.get('memory_buffer', 0):  # NOTE: sys.getsizeof([]) == 56
            await save(config, stream_data[stream]['path'][stream_data[stream]['part']], stream_data[stream]['file_data'])
        stream_data[stream]['file_data'].append(record)

    # NOTE: Closure: no more records
    else:
        # CHANGED FROM FORK
        # Only post-process the current stream, not all of them
        for file_info in [stream_data[stream]]:
            if config.get('memory_buffer') is not None:
                await save(config, file_info['path'][file_info['part']], file_info['file_data'])
            # TODO: post processing
            if post_processing:
                await post_processing(config, file_info['path'][file_info['part']])
            LOGGER.debug("File '%s' saved using open_func '%s'",
                         stream_data[stream]['path'][stream_data[stream]['part']]['absolute_path'], config['open_func'].__name__)



# from io import BytesIO
# from pyarrow import parquet
# from pyarrow.json import read_json
# import pandas
# OUTPUT_PATH: Path = Path(__file__).parent.parent.resolve() / 'output' / 'athena'
# def write_jsonl(input_data: List[Dict], path: str) -> None:
#     OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
#     (OUTPUT_PATH / f'{path}.json.gz').unlink(missing_ok=True)

#     with gzip.open(OUTPUT_PATH / f'{path}.json.gz', 'at', encoding='utf-8') as output_file:
#         output_file.writelines((json.dumps(record, ensure_ascii=False, default=str) + '\n' for record in input_data))


# def read_jsonl(path: str) -> None:

#     # with gzip.open(OUTPUT_PATH / f'{path}.json.gz', 'rt', encoding='utf-8') as input_data:
#     #     return pandas.DataFrame.from_records([json.loads(item) for item in input_data])
#     return pandas.read_json(OUTPUT_PATH / f'{path}.json.gz', lines=True)


# def write_parquet(input_data: List[Dict], path: str) -> None:
#     OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
#     (OUTPUT_PATH / f'{path}.json.parquet').unlink(missing_ok=True)

#     # pandas.json_normalize(input_data).to_parquet(path = OUTPUT_PATH / f'{path}.parquet')
#     # NOTE: synchronous alternative without df middle step, read_json not yet as efficient as pandas. Worth keeping on check.
#     with BytesIO(b''.join(json.dumps(item, ensure_ascii=False, default=str).encode('utf-8') + b'\n' for item in input_data)) as data:
#         parquet.write_table(read_json(data), OUTPUT_PATH / f'{path}.parquet')


# def read_parquet(path: str):

#     return pandas.read_parquet(OUTPUT_PATH / f'{path}.parquet')
