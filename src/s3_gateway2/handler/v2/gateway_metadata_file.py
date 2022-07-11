import json
import urllib.parse
import s3_gateway2.util.handler
import s3_gateway2.util.metadata_id
import s3_gateway2.controller.s3


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/gateway_metadata_file/<gateway.metadata.id>
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
        '_gateway_metadata_file' if params['gateway.metadata.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '400',
        'message': 'Not found.'
    }


# Upload file to root.
# POST /v2/gateway_metadata_file
def _post(environ, params):
    return _post_gateway_metadata_file(environ, params)


# Upload file to folder.
# POST /v2/gateway_metadata_file/<gateway.metadata.id>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _post_gateway_metadata_file(environ, params):

    #
    # Load params.
    #

    params.update({
        'gateway.metadata.name': None,
        'gateway.metadata.modified': None,
        'gateway.metadata.file.size': None,
        'gateway.metadata.file.sha256': None,
    })

    # Load headers.
    if environ.get('HTTP_X_GATEWAY_UPLOAD'):  # wsgi adds HTTP to the header, so client should use X_UPLOAD_JSON
        header_params = json.loads(environ['HTTP_X_GATEWAY_UPLOAD'])
        if header_params:
            if header_params.get('gateway.metadata.name'):
                params['gateway.metadata.name'] = urllib.parse.unquote(header_params['gateway.metadata.name'])
            params['gateway.metadata.file.size'] = header_params.get('gateway.metadata.file.size')
            params['gateway.metadata.modified'] = header_params.get('gateway.metadata.modified')
            params['gateway.metadata.file.sha256'] = header_params.get('gateway.metadata.file.sha256')

    #
    # Validate request.
    #

    # Validate create file params.
    if params['gateway.metadata.file.size'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.metadata.file.size.'
        }
    if not isinstance(params['gateway.metadata.file.size'], int):
        return {
            'code': '400',
            'message': 'Invalid size.'
        }
    if params['gateway.metadata.modified'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.metadata.modified.'
        }
    if not isinstance(params['gateway.metadata.modified'], int):
        return {
            'code': '400',
            'message': 'Invalid gateway.metadata.modified.'
        }

    # Validate file sha256
    if params['gateway.metadata.file.sha256'] and not isinstance(params['gateway.metadata.file.sha256'], str):
        return {
            'code': '400',
            'message': 'Invalid gateway.metadata.file.sha256.'
        }

    #
    # Execute request.
    #

    prefix = s3_gateway2.util.metadata_id.object_key(params['gateway.metadata.id']) \
        if params['gateway.metadata.id'] \
        else None
    if prefix:
        assert prefix[-1] == '/'
    result = s3_gateway2.controller.s3.create_file(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        key_prefix=prefix,
        file_name=params['gateway.metadata.name'],
        size=params['gateway.metadata.file.size'],
        modified=params['gateway.metadata.modified'],
        data=environ['wsgi.input'],
        sha256=params['gateway.metadata.file.sha256'],
    )
    if result is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    # Send new metadata.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(result)
    }


# Update file.
# PUT /v2/gateway_metadata_file/<gateway.metadata.id>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _put_gateway_metadata_file(environ, params):
    assert params.get('gateway.metadata.id')

    #
    # Load.
    #

    params.update({
        'gateway.metadata.file.size': None,
        'gateway.metadata.modified': None,
        'gateway.metadata.file.sha256': None,
    })

    # From headers.
    if environ.get('HTTP_X_GATEWAY_UPLOAD'):  # wsgi adds HTTP to the header, so client should use X_UPLOAD_JSON
        header_params = json.loads(environ['HTTP_X_GATEWAY_UPLOAD'])
        if header_params:
            params['gateway.metadata.modified'] = header_params.get('gateway.metadata.modified')
            params['gateway.metadata.file.size'] = header_params.get('gateway.metadata.file.size')
            params['gateway.metadata.file.sha256'] = header_params.get('gateway.metadata.file.sha256')

    #
    # Validate.
    #

    # Validate type.
    if params['gateway.metadata.file.size'] and not isinstance(params['gateway.metadata.file.size'], int):
        return {
            'code': '400',
            'message': 'Invalid size.'
        }
    if params['gateway.metadata.modified'] and not isinstance(params['gateway.metadata.modified'], int):
        return {
            'code': '400',
            'message': 'Invalid gateway.metadata.modified.'
        }

    # Validate file sha256
    if params['gateway.metadata.file.sha256'] and not isinstance(params['gateway.metadata.file.sha256'], str):
        return {
            'code': '400',
            'message': 'Invalid gateway.metadata.file.sha256.'
        }


    #
    # Execute request.
    #

    # Update
    result = s3_gateway2.controller.s3.update_file(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        object_key=s3_gateway2.util.metadata_id.object_key(params['gateway.metadata.id']),
        size=params['gateway.metadata.file.size'],
        modified=params['gateway.metadata.modified'],
        data=environ['wsgi.input'],
        sha256=params['gateway.metadata.file.sha256'],
    )
    if result is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    # Send new result.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(result)
    }
