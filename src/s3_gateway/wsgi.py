import importlib
import datetime
import logging
import logging.handlers
import time


def dispatch(environ, start_response):

    #
    # Load params.
    #

    params = {
        # From PATH_INFO.
        'version': None,
        'resource': None,
    }

    # Load PATH_INFO
    path_split = environ['PATH_INFO'].split('/')
    params['version'] = path_split[1] if len(path_split) > 1 else None
    params['resource'] = path_split[2] if len(path_split) > 2 else None

    #
    # Dispatch.
    #

    # Get handler.
    resource_module = _get_resource_module(params['version'], params['resource'])
    if resource_module is None:
        # handle unknown module
        return _send_response(start_response, '400', 'Resource Not Found')

    # Delegate.
    resource_handler = getattr(resource_module, 'handle')
    assert resource_handler
    time_start = time.time()
    response = resource_handler(environ)
    time_end = time.time()

    # Inject CORS headers
    if response.get('headers') is None:
        response['headers'] = dict()
    response['headers']['Access-Control-Allow-Origin'] = '*'
    response['headers']['Access-Control-Allow-Headers'] = 'Content-Type, Authorization', 'X-Upload-JSON'
    response['headers']['Access-Control-Allow-Methods'] = 'GET, POST, DELETE, OPTIONS'

    # Log response.
    if _logger:
        _logger.info('{}\t{:.4f}\t{}\t{}\t{}\t{}'.format(
            datetime.datetime.now(),
            time_end - time_start,
            environ.get('REQUEST_METHOD'),
            environ.get('REQUEST_URI'),
            response.get('code') or '',
            response.get('message') or ''))

    # Send http response.
    return _send_response(
        start_response,
        response.get('code'),
        response.get('message'),
        content_type=response.get('contentType'),
        content=response.get('content').encode('utf-8') if response.get('content') else None,
        content_iterator=response.get('contentIterator'),
        headers=response.get('headers')
    )


# load handler module
def _get_resource_module(version, resource):
    handler_module = None

    # load from future if specified
    if version == 'future':
        try:
            handler_module = importlib.import_module('s3_gateway.handler.future.' + resource)
        except ImportError:
            # downgrade version to next lookup
            version = 'downgrade'

    # load from v2 if specified or downgrading
    if version in ['v2', 'downgrade']:
        try:
            handler_module = importlib.import_module('s3_gateway.handler.v2.' + resource)
        except ImportError:
            pass

    return handler_module


# wsgi response
def _send_response(start_response, status_code, status_message, content_type=None, headers=None, content=None,
                   content_iterator=None):
    assert status_code
    assert status_message
    assert not (content and content_iterator)
    assert isinstance(status_code, str)

    # compose headers:
    if headers is None:
        headers = {}

    # add headers for context
    headers['X-Status'] = status_code
    headers['X-Reason'] = status_message

    # add content-type header
    if content_type:
        headers['Content-Type'] = content_type

    # add CORS headers to response
    headers['Access-Control-Allow-Origin'] = '*'
    headers['Access-Control-Allow-Methods'] = 'GET, POST, DELETE, OPTIONS'
    headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'

    # convert unicode headers to set of encoded tuples (header keys and values may not be unicode)
    encoded_headers = []
    for key, value in headers.items():
        if value:
            # filter out empty values
            encoded_headers.append((key, value.encode('unicode-escape').decode('ISO-8859-1')))

    # configure response
    start_response(status_code, encoded_headers)

    # return content generator
    if content_iterator:
        return content_iterator

    # return content with no buffering
    if content:
        return [content]

    # return no content
    return []


#
# Configure.
#

def update_config(config):
    _config.update(config)

    if _config.get('log.enable'):
        global _logger
        assert _config.get('log.path')
        _logger = logging.getLogger(_config['log.path'])
        _logger.setLevel(logging.INFO)
        _logger.addHandler(
            logging.handlers.RotatingFileHandler(
                _config['log.path'],
                maxBytes=1024 * 1024 * 1024,  # 1 gb
                backupCount=2)
        )


_config = {
    'log.enable': True,
    'log.path': 's3_gateway.log'
}

_logger = None
