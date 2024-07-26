from typing import Any, Callable, Collection, Deque, Dict, Iterable, List, Optional, Type
from os.path import join
from decimal import Decimal
import json
from datetime import datetime, timedelta
from collections import deque
from types import TracebackType
from asyncio import Condition, sleep  # , Queue

from aiohttp.web_exceptions import HTTPError
from aiohttp import ClientSession, ClientResponse, BasicAuth, ClientResponseError, ClientError, TCPConnector
import backoff
from .._logger import get_logger


LOGGER = get_logger()
HEADER_RATE_LIMIT: str = 'X-RateLimit-Limit'
HEADER_RATE_LIMIT_REMAINING: str = 'X-RateLimit-Remaining'

RESPONSE_STATUS = {
    400: {
        'message': "Bad Request -- General client error, possibly malformed data."
    },
    401: {
        'message': "Unauthorized -- The API Key was not authorised (or no API Key was found)."
    },
    402: {
        'message': "Payment Required -- The API is not available on your current plan."
    },
    403: {
        'message': "Forbidden -- The request is not allowed."
    },
    404: {
        'message': "Not Found -- The resource was not found."
    },
    405: {
        'message': "Method Not Allowed -- The resource does not accept the HTTP method."
    },
    406: {
        'message': "Not Acceptable -- The resource cannot return the client's required content type."
    },
    408: {
        'message': "Request Timeout -- The server would not wait any longer for the client."
    },
    409: {
        'message': "Conflict - Multiple existing users match this email address - must be more specific using user_id"
    },
    415: {
        'message': "Unsupported Media Type - The server doesn't accept the submitted content-type."
    },
    422: {
        'message': "Unprocessable Entity -- The data was well-formed but invalid."
    },
    429: {
        'message': "Too Many Requests -- The client has reached or exceeded a rate limit, or the server is overloaded."
    },
    500: {
        'message': 'Server errors - something went wrong with the servers. These responses are most likely momentary operational errors'
        '(e.g. temporary unavailability), and, as a result, requests should be retried once.'
    },
    502: {
        'message': 'Server errors - something went wrong with the servers. These responses are most likely momentary operational errors'
        '(e.g. temporary unavailability), and, as a result, requests should be retried once.'
    },
    503: {
        'message': 'Server errors - something went wrong with the servers. These responses are most likely momentary operational errors'
        '(e.g. temporary unavailability), and, as a result, requests should be retried once.'
    },
    504: {
        'message': 'Server errors - something went wrong with the servers. These responses are most likely momentary operational errors'
        '(e.g. temporary unavailability), and, as a result, requests should be retried once.'
    },
}


class Throttler:

    def __init__(self, rate_limit: int, period: float = 1.0) -> None:
        self._rate_limit: int = rate_limit
        self._period: float = period

        self._reset_logs: Deque[datetime] = deque()
        # self._reset_logs: Queue[datetime] = Queue(rate_limit)
        self._condition: Condition = Condition()

    async def __aenter__(self) -> Any:
        await self.acquire()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType]) -> None:
        pass

    async def acquire(self) -> None:
        now: datetime = datetime.utcnow()
        while self._reset_logs:
            async with self._condition:
                now = self.flush()
                # current = self.flush()
                # now = datetime.utcnow()
            if len(self._reset_logs) < self._rate_limit:
                break
            sleep_duration: float = max(0.0, (self._reset_logs[0] - now).total_seconds())
            LOGGER.debug(
                'Throttler rate limit exceeded: %s', json.dumps({
                    'type': 'X-RateLimit',
                    HEADER_RATE_LIMIT: self._rate_limit,
                    HEADER_RATE_LIMIT_REMAINING: self._rate_limit - len(self._reset_logs),
                    'X-RateLimit-Reset': self._reset_logs[0].isoformat(),
                    'X-RateLimit-Period': self._period,
                    'sleep': sleep_duration}))
            await sleep(sleep_duration)

        async with self._condition:
            self._reset_logs.append(now + timedelta(seconds=self._period))
            # self._reset_logs.put(now + timedelta(seconds=self._period))

    # async def flush(self) -> None:
    def flush(self) -> datetime:
        while self._reset_logs and datetime.utcnow() > self._reset_logs[0]:
            self._reset_logs.popleft()
        # current = datetime.utcnow()
        # next = current
        # while not self._reset_logs.empty and datetime.utcnow() > next:

        return datetime.utcnow()

        # sleep_duration: float = 1.0
        # while sleep_duration > 0:
        #     now: datetime = datetime.utcnow()
        #     start_time: datetime = await self._reset_logs.get()
        #     sleep_duration: float = max(0.0, (start_time - now).total_seconds())
        #     if sleep_duration > 0:
        #         await sleep(sleep_duration)


# class RateLimitQuota:

#     def __init__(self, header: str = HEADER_RATE_LIMIT_REMAINING) -> None:
#         self.header = header

#     def __iter__(self) -> Any:
#         return self

#     def __next__(self) -> Any:
#         response = sys.exc_info()[1].response  # type: ignore
#         remaining_quota: int = response.headers.get(self.header, 0)

#         if remaining_quota > 0:
#             raise StopIteration
#         else:
#             LOGGER.info('CLIENT API rate limit exceeded: %s remaining quota before the rate limit reset time.', remaining_quota)
#             return math.floor(float(remaining_quota))

#     def send(self, init: Any = None) -> Any:
#         return self


# NOTE: giveup functions
def is_missing_status(status: List = [404, 429]) -> Callable:

    def is_status(error: ClientResponseError) -> bool:
        return bool(getattr(error, 'status', None) and error.status not in status and error.status < 500)

    return is_status


def raise_for_status(response: ClientResponse, full_url: Optional[str] = None, ignore_status: Collection = []) -> None:

    try:
        response.raise_for_status()
    except (HTTPError, ConnectionError, ClientResponseError) as error:
        LOGGER.error(
            'API Client: %s', json.dumps({
                'type': 'HTTP response', 'status': response.status, 'url': full_url, 'ignore_status': ignore_status,
                'message': RESPONSE_STATUS.get(response.status, {}).get('message', response.reason)}))
        if response.status not in set(ignore_status) | {200, 422}:
            raise error

        # raise APIError(str(error)) from None


def format_response(response: List, params: Dict = {}) -> Any:
    return [r for r in response]


def _retry_pattern() -> Callable:
    return backoff.on_exception(
        backoff.expo,
        (json.decoder.JSONDecodeError, ClientResponseError, ClientError),
        factor=3,
        giveup=is_missing_status([404, 429]),
        max_tries=5,
        logger=LOGGER
    )


class Client:

    def __init__(self, config: Dict) -> None:
        self.config: Dict = config
        self.url: str = config.get('url', '')
        self.headers: Dict[str, Any] = {'Accept': 'application/json'} | config.get('headers', {})
        self._auth: Optional[BasicAuth] = BasicAuth(config.get('login'), config.get('password', '')) if config.get('login') else None

        # NOTE: Allowed 300 requests per minute (5 requests per second), with occasional bursts of up to 20 requests at a time.
        self._throttler: Throttler = Throttler(config.get('rate_limit', 5), config.get('rate_period', 1.0))
        LOGGER.debug('API Client: Throttler %s', json.dumps({'rate_limit': self._throttler._rate_limit, 'rate_period': self._throttler._period}))

    async def __aenter__(self) -> Any:
        return self.connect()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType]
    ) -> None:
        await self.close()

    def connect(self) -> Any:
        # timeout_seconds: int = self.config.get('login', 99)
        # session_timeout: ClientTimeout = ClientTimeout(total=None, sock_connect=timeout_seconds, sock_read=timeout_seconds)
        self.session: ClientSession = ClientSession(
            # NOTE: overwrite the default connector to customise the default connection settings applied to the queries
            connector=TCPConnector(
                # NOTE: max concurrent connections to the end point. 0 by default
                # limit_per_host=0,
                # NOTE: limit on the client connections total count. 100 by default
                # limit=limit_connections_count,
                # NOTE: live connection duration. 30 by default
                # keepalive_timeout=30
            ),
            auth=self._auth,
            headers=self.headers,
            # timeout=session_timeout,
            connector_owner=True)
        LOGGER.debug('API Client: %s', json.dumps({'message': 'API connected'}))
        return self

    async def close(self) -> None:
        await self.session.close()

    @_retry_pattern()
    async def get(self, *args: Optional[Any], **kwargs: Dict) -> Dict:
        path: str = str(args[0])
        full_url: str = join(self.url, str(kwargs.pop('endpoint', path)))
        LOGGER.debug('API Client GET: %s', json.dumps({'message': 'HTTP Request', 'url': full_url, 'params': kwargs.get('params', {})}))

        async with self._throttler, self.session.get(full_url, params=kwargs.pop('params', {})) as response:
            if response.status != 200:
                raise_for_status(response, full_url=full_url, ignore_status=kwargs['ignore_status'] if 'ignore_status' in kwargs else [])
                return {}
            else:
                return json.loads(await response.text(), parse_float=Decimal)
                # NOTE: Alternative return await response.json(loads=partial(json.loads, parse_float=Decimal))

    @_retry_pattern()
    async def post(self, *args: Optional[Any], **kwargs: Dict) -> Dict:
        path: str = str(args[0])
        full_url: str = join(self.url, str(kwargs.pop('endpoint', path)))
        LOGGER.debug('API Client POST: %s', json.dumps({'message': 'HTTP Request', 'url': full_url, 'data': kwargs.get('data', {})}))

        async with self._throttler, self.session.post(
            full_url,
            params=kwargs.pop('params', {}),
            # headers=self.headers,
            # timeout=9,
            data=json.dumps(kwargs.pop('data', {}))
        ) as response:
            if response.status != 200:
                raise_for_status(response, full_url=full_url, ignore_status=kwargs['ignore_status'] if 'ignore_status' in kwargs else [])
                return {}
            else:
                return json.loads(await response.text(), parse_float=Decimal)
                # NOTE: Alternative return await response.json(loads=partial(json.loads, parse_float=Decimal))

    async def get_records(self, tap_stream_id: str, format_response: Callable, *args: Optional[Any], **kwargs: Dict) -> Iterable:

        response = await self.get(tap_stream_id, *args, **kwargs),
        records = format_response(response) if callable(format_response) else []

        return records
