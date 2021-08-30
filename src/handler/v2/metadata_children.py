import base64
import json
import urllib.parse
import util.handler
import controller.s3


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/metadata_children/<content.id>
        'metadata.content.id': environ['PATH_INFO'][22:] if len(environ['PATH_INFO']) > 22 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_metadata_children' if params['metadata.content.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# List root.
# GET /v2/metadata_children
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _get(environ, params):

    #
    # Load.
    #

    params.update({
        'page': None,
    })

    # Query string.
    query_params = urllib.parse.parse_qs(environ.get('QUERY_STRING'))
    params['page'] = query_params.get('page')

    #
    # Execute.
    #

    # List folder content metadata.
    content_list, continuation_token = controller.s3.list_content(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        prefix=None,
        continuation_token=params['page'],
    )
    if content_list is None:
        # handle not allowed
        return None

    # send data as content listing and page token
    return {
        'code': '200',
        'message': 'ok',
        'header': {
            'page': continuation_token
        },
        'contentType': 'application/json',
        'content': json.dumps(content_list)
    }


# List folder.
# GET /v2/metadata_children/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _get_metadata_children(environ, params):
    assert params.get('metadata.content.id')

    #
    # Load.
    #

    params.update({
        'page': None,
    })

    # Query string.
    query_params = urllib.parse.parse_qs(environ.get('QUERY_STRING'))
    params['page'] = query_params.get('page')

    #
    # Validate.
    #
    
    #
    # Execute.
    #

    # List folder content metadata.
    prefix = base64.urlsafe_b64decode(params['metadata.content.id']).decode('utf-8')
    assert prefix[-1] == '/'
    content_list, continuation_token = controller.s3.list_content(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        prefix=prefix,
        continuation_token=params['page'],
    )
    if content_list is None:
        # handle not allowed
        return None

    # send data as content listing and page token
    return {
        'code': '200',
        'message': 'ok',
        'header': {
            'X-Gateway-Page': continuation_token
        },
        'contentType': 'application/json',
        'content': json.dumps(content_list)
    }

