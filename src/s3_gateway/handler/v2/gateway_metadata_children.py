import base64
import json
import urllib.parse
import s3_gateway.util.handler
import s3_gateway.controller.s3


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/gateway_metadata_children/<gateway.metadata.id>
        'gateway.metadata.id': environ['PATH_INFO'][30:] if len(environ['PATH_INFO']) > 30 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_gateway_metadata' if params['gateway.metadata.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# List root.
# GET /v2/gateway_metadata_children
@s3_gateway.util.handler.handle_unexpected_exception
@s3_gateway.util.handler.limit_usage
@s3_gateway.util.handler.handle_requests_exception
@s3_gateway.util.handler.load_access_token
@s3_gateway.util.handler.load_s3_config
@s3_gateway.util.handler.handle_s3_exception
def _get(environ, params):

    #
    # Load.
    #

    params.update({
        'page': None,
    })

    # Query string.
    query_params = urllib.parse.parse_qs(environ.get('QUERY_STRING'))
    params['page'] = query_params['page'][0] if query_params.get('page') else None

    #
    # Execute.
    #

    # List folder content metadata.
    content_list, continuation_token = s3_gateway.controller.s3.list_content(
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
        'headers': {
            'X-Gateway-Page': continuation_token
        },
        'contentType': 'application/json',
        'content': json.dumps(content_list)
    }


# List folder.
# GET /v2/gateway_metadata_children/<gateway.metadata.id>
@s3_gateway.util.handler.handle_unexpected_exception
@s3_gateway.util.handler.limit_usage
@s3_gateway.util.handler.handle_requests_exception
@s3_gateway.util.handler.load_access_token
@s3_gateway.util.handler.load_s3_config
@s3_gateway.util.handler.handle_s3_exception
def _get_gateway_metadata(environ, params):
    assert params.get('gateway.metadata.id')

    #
    # Load.
    #

    params.update({
        'page': None,
    })

    # Query string.
    query_params = urllib.parse.parse_qs(environ.get('QUERY_STRING'))
    params['page'] = query_params['page'][0] if query_params.get('page') else None

    #
    # Validate.
    #
    
    #
    # Execute.
    #

    # List folder content metadata.
    prefix = base64.urlsafe_b64decode(params['gateway.metadata.id']).decode('utf-8')
    assert prefix[-1] == '/'
    content_list, continuation_token = s3_gateway.controller.s3.list_content(
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
        'headers': {
            'X-Gateway-Page': continuation_token
        },
        'contentType': 'application/json',
        'content': json.dumps(content_list)
    }
