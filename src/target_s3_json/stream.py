from pathlib import Path
import sys
import argparse
from typing import Any, Callable, Dict, Generator, TextIO, Tuple, Optional
from decimal import Decimal, getcontext
from math import ceil, floor
import json
import datetime
from asyncio import run
from io import TextIOWrapper
from copy import deepcopy

from jsonschema import Draft4Validator, FormatChecker

from .file import set_schema, save_json, config_file, config_compression
from target._logger import get_logger
LOGGER = get_logger()


def _add_metadata_columns_to_schema(schema_message: Dict) -> Dict:
    '''Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    '''
    schema_message['schema']['properties'].update(
        _sdc_batched_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_deleted_at={'type': ['null', 'string']},
        _sdc_extracted_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_primary_key={'type': ['null', 'string']},
        _sdc_received_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_sequence={'type': ['integer']},
        _sdc_table_version={'type': ['null', 'string']})

    return schema_message


def _add_metadata_values_to_record(record_message: Dict, schema_message: Dict, timestamp: datetime.datetime) -> Dict:
    '''Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    '''
    utcnow = timestamp.astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat()

    record_message['record'].update(
        _sdc_batched_at=utcnow,
        _sdc_deleted_at=record_message['record'].get('_sdc_deleted_at'),
        _sdc_extracted_at=record_message.get('time_extracted'),
        _sdc_primary_key=schema_message.get('key_properties'),
        _sdc_received_at=utcnow,
        _sdc_sequence=int(timestamp.timestamp() * 1000),
        _sdc_table_version=record_message.get('version'))

    return record_message['record']


def _remove_metadata_values_from_record(record_message: Dict) -> Dict:
    '''Removes every metadata _sdc column from a given record message
    '''
    for key in {
        '_sdc_batched_at',
        '_sdc_deleted_at',
        '_sdc_extracted_at',
        '_sdc_primary_key',
        '_sdc_received_at',
        '_sdc_sequence',
        '_sdc_table_version'
    }:

        record_message['record'].pop(key, None)

    return record_message['record']


# def float_to_decimal(value):
#     '''Walk the given data structure and turn all instances of float into
#     double.'''
#     if isinstance(value, float):
#         return Decimal(str(value))
#     if isinstance(value, list):
#         return [float_to_decimal(child) for child in value]
#     if isinstance(value, dict):
#         return {k: float_to_decimal(v) for k, v in value.items()}
#     return value


def _is_precision_available(schema: Any) -> bool:
    if 'type' not in schema:
        return False
    if isinstance(schema['type'], list):
        if 'number' not in schema['type']:
            return False
    elif schema['type'] != 'number':
        return False
    if 'multipleOf' in schema:
        return True
    return 'minimum' in schema or 'maximum' in schema


def _get_precision(value: Optional[Any] = None) -> int:
    v = abs(Decimal(value or 1)).log10()
    return abs(round(floor(v)) if v < 0 else round(ceil(v)))


def _all_precisions(schema: Optional[Any] = None) -> Generator:
    if isinstance(schema, list):
        for s in schema:
            for precision in _all_precisions(s):
                yield precision
    elif isinstance(schema, dict):
        if _is_precision_available(schema):
            # NOTE: yield precision = digit + scale
            yield max(_get_precision(schema.get('minimum')), _get_precision(schema.get('maximum'))) \
                + -1 * _get_precision(schema.get('multipleOf'))
        else:
            for s in schema.values():
                for precision in _all_precisions(s):
                    yield precision


def emit_state(state: Optional[Any]) -> None:
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug(f'Emitting state {line}')
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()


class Loader():

    def __init__(self,
                 config: Dict,
                 set_schemas: Callable = set_schema,
                 writeline: Callable = save_json) -> None:
        self.config: Dict = config
        self.set_schemas: Callable = set_schemas
        self.writeline: Callable = writeline

    async def writelines(self, lines: TextIO) -> Tuple[Optional[Any], Dict[Any, Any]]:
        '''Process the lines received from the Singer Tap.

        This is the core of the lines processing.
        Each line is processed function of its type, and the *RECORD* lines are saved using and functions provided as an argument.
        By default they are written as a *jsonl* in the *work_dir* working directory provided in the default `config` file.

        Parameters
        ----------
        lines: TextIO
            Input IO stream of lines
        config : dict
            configuration dictionary.
        set_schemas : Callable
            function used to process the schemas
        writeline : Callable
            function used to save the records

        Raises
        ------
        json.decoder.JSONDecodeError
            If the line structure is inconsistent or cnotains errors.

        Returns
        -------
        out : list[dict, dict]
            A `state` closure info.

        See Also
        --------
        `Singer spec <https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md>`_ *convention* and *protocol*.
        '''

        self.state = None
        stream: Optional[str] = None
        schemas = {}
        key_properties = {}
        validators: Dict = {}
        self.stream_data: Dict = {}

        lastStateLine = ''

        for raw_line in lines:
            try:
                line = json.loads(raw_line)  # NOTE: optional argument parse_float=Decimal)
            except json.decoder.JSONDecodeError:
                LOGGER.error(f'Unable to parse:\n{raw_line}')
                raise

            message_type = line['type']
            if message_type == 'SCHEMA':
                # CHANGED FROM FORK
                # copy before changes
                origline = deepcopy(line)

                if 'stream' not in line:
                    raise Exception(f"Line is missing required key 'stream': {raw_line}")

                if not self.config.get('asynchronous', True) and stream is not None:
                    await self.writeline(stream, self.stream_data, self.config)

                stream = line['stream']

                # schemas[stream] = _add_metadata_columns_to_schema(line) if self.config.get('add_metadata_columns') else _float_to_decimal(line['schema'])
                schemas[stream] = _add_metadata_columns_to_schema(line) if self.config.get('add_metadata_columns') else line

                getcontext().prec = max(_all_precisions(line['schema']), default=max(1, getcontext().prec))
                LOGGER.debug('Setting decimal precision to {}'.format(getcontext().prec))


                # NOTE: prevent exception *** jsonschema.exceptions.UnknownType: Unknown type 'SCHEMA' for validator.
                #       'type' is a key word for jsonschema validator which is different from `{'type': 'SCHEMA'}` as the message type.

                schemas[stream].pop('type')
                validators[stream] = Draft4Validator(schemas[stream], format_checker=FormatChecker())

                if 'key_properties' not in line:
                    raise Exception('key_properties field is required')
                key_properties[stream] = line['key_properties']
                LOGGER.debug('Setting schema for {}'.format(stream))

                self.set_schemas(stream, self.config, self.stream_data, schemas[stream])

                # CHANGED FROM FORK
                # Also write the schema line to the appropriate file, do this before popping 'type'
                await self.writeline(stream, self.stream_data, self.config, origline)

            elif message_type == 'ACTIVATE_VERSION':
                LOGGER.debug(f'ACTIVATE_VERSION {raw_line}')

                # if version not in line:
                #     raise Exception(f'An ACTIVATE_VERSION value is expected for stream {stream}')

                # active_version = line['version']

            elif message_type == 'RECORD':
                if 'stream' not in line:
                    raise Exception(f"Line is missing required key 'stream': {raw_line}")
                stream = line['stream']

                if stream not in schemas:
                    raise Exception(f'A record for stream {stream} was encountered before a corresponding schema')

                record_to_load = line['record']

                # NOTE: Validate record
                # validators[stream].validate(_float_to_decimal(record_to_load))
                validators[stream].validate(record_to_load)

                # CHANGED FROM FORK
                # Just write the whole raw line
                await self.writeline(stream, self.stream_data, self.config, line)
                '''
                record_to_load = _add_metadata_values_to_record(line, {}, self.config['date_time']) \
                    if self.config.get('add_metadata_columns') \
                    else _remove_metadata_values_from_record(line)

                await self.writeline(stream, self.stream_data, self.config, record_to_load)
                '''

                self.state = None

            elif message_type == 'STATE':
                lastStateLine = line
                self.state = line['value']
                # Need to write these so we can tell if we got an incomplete stream
                #await self.writeline(stream, self.stream_data, self.config, line)

            else:
                LOGGER.warning('Unknown line type "{}" in line "{}"'.format(line['type'], line))

        for stream in self.stream_data:
            # Write last state line at the end of every file so that we know it is complete
            if lastStateLine:
                await self.writeline(stream, self.stream_data, self.config, lastStateLine)
            await self.writeline(stream, self.stream_data, self.config)

        return self.state, self.stream_data

    async def sync(self, lines: TextIO) -> None:

        await self.writelines(lines)

    def run(self, lines: TextIO = sys.stdin) -> None:

        # hasattr(lines, 'reconfigure') and lines.reconfigure(encoding='utf-8')  # type: ignore
        run(self.sync(TextIOWrapper(lines.buffer, encoding='utf-8')))

        emit_state(self.state)
        LOGGER.debug('Completed successfully')


def main(lines: TextIO = sys.stdin) -> None:
    '''Main'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()
    config = config_compression(config_file(json.loads(Path(args.config).read_text(encoding='utf-8'))))

    Loader(config).run(lines)
