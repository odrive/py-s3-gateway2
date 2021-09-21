import json
import util.handler
import util.metadata_id
import controller.s3


def handle(environ):

    #
    # Load params.
    #

    params = {
        # From PATH_INFO: /v2/gateway_metadata/<gateway.metadata.id>
        'gateway.metadata.id': environ['PATH_INFO'][21:] if len(environ['PATH_INFO']) > 21 else None,
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


# Delete root folder.
# DELETE /v2/gateway_metadata
def _delete(environ, params):
    # Not allowed.
    return {
        'code': '403',
        'message': 'Not allowed.',
    }


# Delete file or folder.
# DELETE /v2/gateway_metadata/<gateway.metadata.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _delete_gateway_metadata(environ, params):
    assert params.get('gateway.metadata.id')

    # Delete file.
    object_key = util.metadata_id.object_key(params['gateway.metadata.id'])
    if object_key:
        result = controller.s3.delete_file(
            region=params['config.region'],
            host=params['config.host'],
            access_key=params['config.access.key'],
            access_key_secret=params['config.access.key.secret'],
            bucket=params['config.bucket'],
            object_key=object_key
        )
        if result is False:
            # Not allowed.
            return {
                'code': '403',
                'message': 'Not allowed.'
            }
        
        # Success
        return {
            'code': '200',
            'message': 'OK'
        }

    # Delete folder.
    prefix = util.metadata_id.object_key(params['gateway.metadata.id'])
    if prefix:
        result = controller.s3.delete_folder(
            region=params['config.region'],
            host=params['config.host'],
            access_key=params['config.access.key'],
            access_key_secret=params['config.access.key.secret'],
            bucket=params['config.bucket'],
            object_prefix=prefix
        )
        if result is False:
            # Not allowed
            return {
                'code': '403',
                'message': 'Not allowed.'
            }

        # Success.
        return {
            'code': '200',
            'message': 'OK'
        }

    # Invalid gateway.metadata.id.
    return {
        'code': '400',
        'message': 'Invalid gateway.metadata.id'
    }


# Get metadata for root folder.
# GET /v2/gateway_metadata
@util.handler.limit_usage
def _get(environ, params):
    # Get root folder metadata.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps({
            'gateway.metadata.id': '',
            'gateway.metadata.type': 'folder',
            'gateway.metadata.name': '',
            'gateway.metadata.modified': None,
        })
    }


# Get file or folder metadata.
# GET /v2/gateway_metadata/<gateway.metadata.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _get_gateway_metadata(environ, params):
    assert params.get('gateway.metadata.id')

    #
    # Load.
    #

    #
    # Validate.
    #

    #
    # Execute.
    #

    object_key = util.metadata_id.object_key(params['metadata.content.id'])

    # Handle folder.
    if object_key[-1] == '/':
        return {
            'code': '200',
            'message': 'ok',
            'contentType': 'application/json',
            'content': json.dumps({
                'gateway.metadata.id': util.metadata_id.metadata_id(object_key),
                'gateway.metadata.type': 'folder',
                'gateway.metadata.name': params['prefix'].rstrip('/').split('/')[-1],
                'gateway.metadata.modified': None,
            })
        }

    # Handle file.
    assert object_key[-1] != '/'
    result = controller.s3.get_file_metadata(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        object_key=object_key
    )
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(result)
    }
