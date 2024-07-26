from pathlib import Path
import sys
from typing import Optional, Set
from typing import Dict, TextIO, Any  # , Callable
import argparse
import json
# from asyncio import run, shield, Condition, gather

from ..stream import Loader
# from .. import file
from ..file import config_file
from .._logger import get_logger
from .client import Client

LOGGER = get_logger()

CONFIG_KEYS_REQUIRED: Set[str] = {
    'url'
}


def format_data(input_data: Dict) -> Dict:
    return input_data


def set_persist(stream: str, config: Dict, stream_data: Dict, schema: Dict = {}) -> Dict:
    # NOTE: get the file key. Persistent array data storage.
    if stream not in stream_data:
        stream_data[stream] = {
            'schema': schema,
            'endpoint': config.get('post_params', {}).get(stream, {}).get('endpoint', ''),
            'params': config.get('post_params', {}).get(stream, {}).get('params', ''),
            'format_data': format_data,
            'api_data': []}
    return stream_data


async def save_json(stream: str, stream_data: Dict, config: Dict[str, Any], record: Optional[Dict[Any, Any]] = None) -> None:
    if record:
        stream_write = stream_data[stream]
        endpoint = stream_write['endpoint'].format(**record)
        params = stream_write.get('params', {})
        formatted_data = stream_write['format_data'](record)

        async with config['client'] as client:
            response = await client.post(stream, endpoint=endpoint, params=params, data=formatted_data)

            return response


# class APILoader(Loader):

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.config['client'] = Client(self.config)

#     async def sync(self, lines: TextIO = sys.stdin) -> None:
#         # client = create_session(self.config).client('s3', **({'endpoint_url': self.config.get('aws_endpoint_url')} \
#         #     if self.config.get('aws_endpoint_url') else {}))
#         # async with create_session(self.config).create_client('s3', **({'endpoint_url': self.config.get('aws_endpoint_url')} \
#         #     if self.config.get('aws_endpoint_url') else {})) as client:
#         await self.writelines(lines)

#         # await self.writelines(lines, writeline=partial(save_json, save=partial(put_object, client=client)))

#         # await upload_files(self.stream_data, self.config)


def main(loader: type[Loader] = Loader, lines: TextIO = sys.stdin) -> None:
    '''Main'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()
    config = config_file(json.loads(Path(args.config).read_text(encoding='utf-8')))

    loader(config | {'client': Client(config)}, set_schemas=set_persist, writeline=save_json).run(lines)
