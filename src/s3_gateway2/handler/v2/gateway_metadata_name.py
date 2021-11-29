import json
import s3_gateway2.util.handler
import s3_gateway2.util.metadata_id
import s3_gateway2.controller.s3


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/gateway_metadata_name/<gateway.metadata.id>
        'gateway.metadata.id': environ['PATH_INFO'][26:] if len(environ['PATH_INFO']) > 26 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_gateway_metadata_name' if params['gateway.metadata.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '400',
        'message': 'Not found.'
    }


# Rename file or folder.
# PUT /v2/gateway_metadata_name/<gateway.metadata.id>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _put_gateway_metadata_name(environ, params):
    assert params.get('gateway.metadata.id')

    #
    # Load.
    #

    params.update({
        'new.gateway.metadata.name': None,
        'old.gateway.metadata.name': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['new.gateway.metadata.name'] = body.get('new.gateway.metadata.name')
    params['old.gateway.metadata.name'] = body.get('old.gateway.metadata.name')

    #
    # Validate.
    #

    # Validate name.
    if params['new.gateway.metadata.name'] is None:
        return {
            'code': '400',
            'message': 'Missing new.gateway.metadata.name'
        }

    #
    # Execute.
    #

    result = s3_gateway2.controller.s3.rename(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        object_key=s3_gateway2.util.metadata_id.object_key(params['gateway.metadata.id']),
        new_name=params['new.gateway.metadata.name'],
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
            'gateway.metadata.name': result['gateway.metadata.name']
        })
    }
