import time
import urllib
import urllib.parse
import requests
import traceback
import xmltodict
import s3_gateway.util.s3
import s3_gateway.controller.datastore


def handle_requests_exception(dispatch_func):
    def wrapper(environ, params):
        try:
            return dispatch_func(environ, params)

        except requests.Timeout:
            return {'code': '504', 'message': 'Network Timeout'}

        except requests.ConnectionError:
            return {'code': '504', 'message': 'Network Connection Error'}

    return wrapper


def handle_unexpected_exception(dispatch_func):
    def wrapper(*args, **kwargs):
        try:
            # execute wrapped function
            return dispatch_func(*args, **kwargs)

        except Exception as e:

            # notify
            _print_unexpected_exception(e)

            # handle as internal server error
            return {'code': '500', 'message': 'Unexpected Error'}

    return wrapper


def load_access_token(dispatch_func):
    def wrapper(environ, params):
        # Load.
        params['access.token'] = _get_access_token(environ)

        # Validate.
        if params['access.token'] is None:
            return {
                'code': '401',
                'message': 'Unauthorized.'
            }

        return dispatch_func(environ, params)

    return wrapper


def load_s3_config(dispatch_func):
    def wrapper(environ, params):
        assert params['access.token']
        config = s3_gateway.controller.datastore.get(params['access.token'], 'registration')

        # Validate
        if config is None:
            # Invalid access token.
            return {
                'code': '401',
                'message': 'Unauthorized.'
            }

        # Load.
        assert config.get('config.region')
        assert config.get('config.host')
        assert config.get('config.access.key')
        assert config.get('config.access.key.secret')
        params.update(config)

        # Delegate.
        return dispatch_func(environ, params)

    return wrapper


def handle_s3_exception(dispatch_func):
    def wrapper(*args, **kwargs):
        try:
            return dispatch_func(*args, **kwargs)

        except s3_gateway.util.s3.S3Exception as e:

            if e.http_response.status_code == 400:
                # Parse message.
                message = 'S3 Bad Request'
                content = e.http_response.content
                if content and content != '':
                    response_dict = xmltodict.parse(content)
                    if 'Message' in response_dict:
                        message = response_dict['Message']

                # Raise error.
                return {'code': '400', 'message': message}

            if e.http_response.status_code == 401:
                return {'code': '401', 'message': 'Invalid S3 credentials.'}

            if e.http_response.status_code == 403:
                return {'code': '403', 'message': 'Not allowed by S3.'}

            if e.http_response.status_code == 404:
                return {'code': '404', 'message': 'S3 not found.'}

            if e.http_response.status_code in [500, 502, 503]:
                return {'code': '502', 'message': 'S3 unavailable.'}

            if e.http_response.status_code == 504:
                return {'code': '504', 'message': 'S3 unavailable.'}

            # handle unexpected
            raise

    return wrapper


def limit_usage(dispatch_func):
    def wrapper(*args, **kwargs):

        global _usage_count
        global _usage_start

        # reset usage after usage interval
        if time.time() > _usage_start + _config['usage.interval.seconds']:
            _usage_start = time.time()
            _usage_count = 0

        _usage_count += 1

        # check if exceed max requests within usage interval
        if _usage_count > _config['usage.count.max']:
            return {'code': '429', 'message': 'Exceeded usage limit'}

        # execute wrapped function
        return dispatch_func(*args, **kwargs)

    return wrapper


def _print_unexpected_exception(exception):
    print('')
    print('-------unhandled exception---------')
    print(traceback.print_exc())
    print(exception)

    # report uncaught HTTPError
    if isinstance(exception, requests.HTTPError):
        print('')
        print('HTTP ERROR')
        print('URL: ' + exception.response.url)
        print('Status Code: ' + str(exception.response.status_code))
        print('Reason: ' + exception.response.reason)

        for header in exception.response.headers:
            if header.lower().startswith('x-odrive'):
                print(header + ': ' + exception.response.headers[header])
    print('')


def _get_access_token(environ):
    assert environ

    # Load from HTTP header.
    if 'HTTP_AUTHORIZATION' in environ:
        # load access token from header
        http_authorization_split = environ['HTTP_AUTHORIZATION'].split(' ')
        access_token = http_authorization_split[1] if len(http_authorization_split) > 1 else None
        if access_token:
            return access_token

    # Load from query string.
    query_params = urllib.parse.parse_qs(environ.get('QUERY_STRING'))
    if query_params.get('aut'):
        access_token = query_params['aut'][0]
        if access_token:
            return access_token

    # Load from cookie.
    access_token = _get_cookie('s', environ)
    if access_token:
        return access_token

    # No session ID.
    return None


def _get_cookie(name, environ):
    cookies = environ.get('HTTP_COOKIE')
    if cookies is None:
        return None

    # convert 'aaa=bbb; ccc=ddd' into {'aaa': 'bbb', 'ccc': 'ddd'}
    cookie_list = [item.split('=') for item in cookies.split(';')]
    cookie_map = {cookie[0].strip(' '): cookie[1].strip(' ') for cookie in cookie_list}

    return cookie_map.get(name)


#
# config
#

def update_config(config):
    assert config.get('usage.interval.seconds')
    assert config.get('usage.count.max')
    _config.update(config)


_config = {
    'usage.interval.seconds': 10,  # number of seconds
    'usage.count.max': 1000,  # max requests within usage interval
}

_usage_start = time.time()
_usage_count = 0
