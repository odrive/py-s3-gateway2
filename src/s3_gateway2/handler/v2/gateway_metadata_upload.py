import json

import s3_gateway2.controller.s3
import s3_gateway2.util.handler
import s3_gateway2.util.metadata_id


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/gateway_metadata_upload/<gateway.metadata.id>
        'gateway.metadata.id': environ['PATH_INFO'][28:] if len(environ['PATH_INFO']) > 28 else None,
    }

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_gateway_metadata_upload' if params['gateway.metadata.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '400',
        'message': 'Not found.'
    }


# Upload large file to folder.
# POST /v2/gateway_metadata_upload/<gateway.metadata.id>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _post_gateway_metadata_upload(environ, params):
    assert params['gateway.metadata.id']

    #
    # Load params.
    #

    params.update({
        'gateway.metadata.file.size': None,
        'gateway.upload.id': None,
        'gateway.upload.segment': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['gateway.metadata.file.size'] = body.get('gateway.metadata.file.size')
    params['gateway.upload.id'] = body.get('gateway.upload.id')
    params['gateway.upload.segment'] = body.get('gateway.upload.segment')

    #
    # Validate request.
    #

    # Validate size.
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

    # Validate upload session ID
    if params['gateway.upload.id'] and not isinstance(params['gateway.upload.id'], str):
        return {
            'code': '400',
            'message': 'Invalid gateway.upload.id.'
        }

    # Validate upload segment list
    if params['gateway.upload.segment']:
        if not isinstance(params['gateway.upload.segment'], list):
            return {
                'code': '400',
                'message': 'Invalid gateway.upload.segment list.'
            }

    #
    # Execute request.
    #

    # Complete the upload.
    new_metadata = s3_gateway2.controller.s3.complete_upload(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        gateway_upload_id=params['gateway.upload.id'],
        segments=params['gateway.upload.segment'],
        size=params['gateway.metadata.file.size']
    )
    if new_metadata is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    # Send new metadata.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(new_metadata)
    }


# Update large file.
# PUT /v2/gateway_metadata_upload/<gateway.metadata.id>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _put_gateway_metadata_upload(environ, params):
    assert params.get('gateway.metadata.id')

    #
    # Load.
    #

    params.update({
        'gateway.metadata.file.size': None,
        'gateway.upload.id': None,
        'gateway.upload.segment': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['gateway.metadata.file.size'] = body.get('gateway.metadata.file.size')
    params['gateway.upload.id'] = body.get('gateway.upload.id')
    params['gateway.upload.segment'] = body.get('gateway.upload.segment')

    #
    # Validate.
    #

    # Validate size.
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

    # Validate upload session ID
    if params['gateway.upload.id'] and not isinstance(params['gateway.upload.id'], str):
        return {
            'code': '400',
            'message': 'Invalid gateway.upload.id.'
        }

    # Validate upload segment list
    if params['gateway.upload.segment']:
        if not isinstance(params['gateway.upload.segment'], list):
            return {
                'code': '400',
                'message': 'Invalid gateway.upload.segment list.'
            }

    #
    # Execute request.
    #

    # Complete the upload.
    updated_metadata = s3_gateway2.controller.s3.complete_upload(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        gateway_upload_id=params['gateway.upload.id'],
        segments=params['gateway.upload.segment'],
        size=params['gateway.metadata.file.size']
    )
    if updated_metadata is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    # Send new metadata.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(updated_metadata)
    }
