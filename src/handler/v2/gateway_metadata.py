import json
import urllib.parse
import util.handler
import util.metadata_id
import controller.s3
import requests_toolbelt


def handle(environ):

    #
    # Load params.
    #

    params = {
        # From PATH_INFO: /v2/metadata/<content.id>
        'metadata.content.id': environ['PATH_INFO'][13:] if len(environ['PATH_INFO']) > 13 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_metadata' if params['metadata.content.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Delete root folder.
# DELETE /v2/metadata
def _delete(environ, params):
    # Not allowed.
    return {
        'code': '403',
        'message': 'Not allowed.',
    }


# Delete file or folder.
# DELETE /v2/metadata/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _delete_metadata(environ, params):
    assert params.get('metadata.content.id')

    # Delete file.
    object_key = util.metadata_id.object_key(params['metadata.content.id'])
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
    prefix = util.metadata_id.object_key(params['metadata.content.id'])
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

    # Invalid content.id.
    return {
        'code': '400',
        'message': 'Invalid content.id'
    }


# Get metadata for root folder.
# GET /v2/metadata
@util.handler.limit_usage
def _get(environ, params):
    # Get root folder metadata.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps({
            'metadata.content.id': '',
            'metadata.content.type': 'folder',
            'metadata.content.name': '',
        })
    }


# Get file or folder metadata.
# GET /v2/metadata/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _get_metadata(environ, params):
    assert params.get('metadata.content.id')

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
                'metadata.content.id': util.metadata_id.metadata_id(object_key),
                'metadata.content.type': 'folder',
                'metadata.content.name': params['prefix'].rstrip('/').split('/')[-1]
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


# Update file.
# PUT /v2/metadata_file/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _put_metadata(environ, params):
    assert params.get('metadata.content.id')

    #
    # Load.
    #

    params.update({
        'metadata.file.size': None,
        'metadata.content.modified': None,
    })

    # From headers.
    if environ.get('HTTP_X_GATEWAY_UPLOAD'):  # wsgi adds HTTP to the header, so client should use X_UPLOAD_JSON
        header_params = json.loads(environ['HTTP_X_GATEWAY_UPLOAD'])
        if header_params:
            params['metadata.content.modified'] = header_params.get('metadata.content.modified')
            params['metadata.file.size'] = header_params.get('metadata.file.size')

    #
    # Validate.
    #

    # Validate type.
    if params['metadata.file.size'] and not isinstance(params['metadata.file.size'], int):
        return {
            'code': '400',
            'message': 'Invalid size.'
        }
    if params['metadata.content.modified'] and not isinstance(params['metadata.content.modified'], int):
        return {
            'code': '400',
            'message': 'Invalid content.modified.'
        }

    #
    # Execute request.
    #

    # Update
    updated_content = controller.s3.update_file(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        object_key=util.metadata_id.object_key(params['metadata.content.id']),
        size=params['metadata.file.size'],
        modified=params['metadata.content.modified'],
        data=environ['wsgi.input'],
    )
    if updated_content is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    # Send new content.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(updated_content)
    }