#!/usr/bin/env python3

__version__ = '0.1.0'

from .stream import main
from ._logger import get_logger
LOGGER = get_logger()

CONFIG_PARAMS = {
    'add_metadata_columns',
    'path_template',
    'timezone_offset',
    'memory_buffer',
    'work_dir',
    'compression',
    'file_type'
}


if __name__ == '__main__':
    main()
