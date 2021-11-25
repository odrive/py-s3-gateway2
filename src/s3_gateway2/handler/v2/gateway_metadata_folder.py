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
        # URI /v2/gateway_metadata_folder/<gateway.metadata.id>
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


# Create root sub folder.
# POST /v2/gateway_metadata_folder
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _post(environ, params):
    #
    # Load.
    #

    params.update({
        'gateway.metadata.name': None,
        'gateway.metadata.modified': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['gateway.metadata.name'] = body.get('gateway.metadata.name')
    params['gateway.metadata.modified'] = body.get('gateway.metadata.modified')

    #
    # Validate.
    #

    # Validate name.
    if params['gateway.metadata.name'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.metadata.name.'
        }

    # S3 does not support modified. So ignore.
    # if params['gateway.metadata.modified'] is None:
    #     return {
    #         'code': '400',
    #         'message': 'Missing gateway.metadata.modified.'
    #     }
    # if not isinstance(params['gateway.metadata.modified'], int):
    #     return {
    #         'code': '400',
    #         'message': 'Invalid gateway.metadata.modified.'
    #     }

    #
    # Execute.
    #

    new_folder = s3_gateway2.controller.s3.create_folder(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        key_prefix=None,
        folder_name=params['gateway.metadata.name']
    )
    if new_folder is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(new_folder)
    }


# Create sub folder.
# POST /v2/gateway_metadata_folder/<gateway.metadata.id>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _post_gateway_metadata(environ, params):

    #
    # Load.
    #

    params.update({
        'gateway.metadata.name': None,
        'gateway.metadata.modified': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['gateway.metadata.name'] = body.get('gateway.metadata.name')
    # params['gateway.metadata.modified'] = body.get('gateway.metadata.modified')

    #
    # Validate.
    #

    # Validate name.
    if params['gateway.metadata.name'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.metadata.name.'
        }

    # S3 does not support modified. So ignore.
    # if params['gateway.metadata.modified'] is None:
    #     return {
    #         'code': '400',
    #         'message': 'Missing gateway.metadata.modified.'
    #     }
    # if not isinstance(params['gateway.metadata.modified'], int):
    #     return {
    #         'code': '400',
    #         'message': 'Invalid gateway.metadata.modified.'
    #     }

    #
    # Execute.
    #

    new_folder = s3_gateway2.controller.s3.create_folder(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        key_prefix=s3_gateway2.util.metadata_id.object_key(params['gateway.metadata.id']),
        folder_name=params['gateway.metadata.name']
    )
    if new_folder is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(new_folder)
    }
