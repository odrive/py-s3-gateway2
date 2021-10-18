import json
import s3_gateway.util.handler
import s3_gateway.util.metadata_id
import s3_gateway.controller.s3


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/gateway_metadata_parent/<gateway.metadata.id>
        'gateway.metadata.id': environ['PATH_INFO'][28:] if len(environ['PATH_INFO']) > 28 else None,
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


# Move file or folder.
# PUT /v2/gateway_metadata_parent/<gateway.metadata.id>
@s3_gateway.util.handler.handle_unexpected_exception
@s3_gateway.util.handler.limit_usage
@s3_gateway.util.handler.handle_requests_exception
@s3_gateway.util.handler.load_access_token
@s3_gateway.util.handler.load_s3_config
@s3_gateway.util.handler.handle_s3_exception
def _put_gateway_metadata(environ, params):
    assert params.get('gateway.metadata.id')

    #
    # Load.
    #

    params.update({
        'new.gateway.metadata.parent.id': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['new.gateway.metadata.parent.id'] = body.get('new.gateway.metadata.parent.id')

    #
    # Validate.
    #

    # Check new parent.
    if params['new.gateway.metadata.parent.id'] is None:
        return {
            'code': '400',
            'message': 'Missing new.gateway.metadata.parent.id.'
        }

    # Check source.
    if s3_gateway.util.metadata_id.object_key(params['gateway.metadata.id'])[-1] == '/':
        # Not allowed to move folder.
        return {
            'code': '403',
            'message': 'Not allowed to move folder.'
        }

    #
    # Execute.
    #

    result = s3_gateway.controller.s3.move(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        object_key=s3_gateway.util.metadata_id.object_key(params['gateway.metadata.id']),
        new_prefix=s3_gateway.util.metadata_id.object_key(params['new.gateway.metadata.parent.id'])
    )
    if result is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps({
            'gateway.metadata.id': result['gateway.metadata.id'],
            'gateway.metadata.parent.id': result['gateway.metadata.parent.id'],
        })
    }
