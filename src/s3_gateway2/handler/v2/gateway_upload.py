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
        # URI /v2/gateway_upload/<gateway.upload.id>
        'gateway.upload.id': environ['PATH_INFO'][19:] if len(environ['PATH_INFO']) > 19 else None,
    }

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_gateway_upload' if params['gateway.upload.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '400',
        'message': 'Not found.'
    }


# Start large upload session.
# POST /v2/gateway_upload
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _post(environ, params):
    #
    # Load params.
    #

    params.update({
        'gateway.metadata.id': None,
        'gateway.metadata.parent.id': None,
        'gateway.metadata.name': None,
        'gateway.metadata.modified': None,
        'gateway.metadata.file.size': None,
        'gateway.metadata.file.sha256': None,
        'gateway.upload.segment': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['gateway.metadata.id'] = body.get('gateway.metadata.id')
    params['gateway.metadata.parent.id'] = body.get('gateway.metadata.parent.id')
    params['gateway.metadata.name'] = body.get('gateway.metadata.name')
    params['gateway.metadata.file.size'] = body.get('gateway.metadata.file.size')
    params['gateway.metadata.modified'] = body.get('gateway.metadata.modified')
    params['gateway.metadata.file.sha256'] = body.get('gateway.metadata.file.sha256')
    params['gateway.upload.segment'] = body.get('gateway.upload.segment')

    #
    # Validate request.
    #

    # Validate parent id..
    if params['gateway.metadata.parent.id'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.metadata.parent.id.'
        }

    # Validate name..
    if params['gateway.metadata.name'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.metadata.name.'
        }
    # if any(u in params['gateway.metadata.name'] for u in ['"', '*', ':' , '<', '>', '?', '\\', '/', '|']):
    #     return {
    #         'code': '403',
    #         'message': 'File name contains illegal characters (" * : < > ? / \ |) for s3.'
    #     }

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

    # Validate mod time
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

    # Validate upload segment list
    if not isinstance(params['gateway.upload.segment'], list):
        return {
            'code': '400',
            'message': 'Invalid gateway.upload.segment list.'
        }
    for entry in params['gateway.upload.segment']:
        if not isinstance(entry.get('gateway.upload.segment.number'), int):
            return {
                'code': '400',
                'message': 'Invalid or missing gateway.upload.segment.number in gateway.upload.segment.'
            }
        if entry['gateway.upload.segment.number'] < 1:
            return {
                'code': '400',
                'message': 'gateway.upload.segment.number must be > 0'
            }
        if not isinstance(entry.get('gateway.upload.segment.sha256'), str):
            return {
                'code': '400',
                'message': 'Invalid or missing gateway.upload.segment.sha256 in gateway.upload.segment.'
            }
        if not isinstance(entry.get('gateway.upload.segment.size'), int):
            return {
                'code': '400',
                'message': 'Invalid or missing gateway.upload.segment.size in gateway.upload.segment.'
            }

    #
    # Execute request.
    #

    # Determine if we have an item or a folder.
    if params['gateway.metadata.id']:

        # Create upload for item.
        new_upload = s3_gateway2.controller.s3.create_update_file_upload(
            region=params['config.region'],
            host=params['config.host'],
            access_key=params['config.access.key'],
            access_key_secret=params['config.access.key.secret'],
            bucket=params['config.bucket'],
            object_key=s3_gateway2.util.metadata_id.object_key(params['gateway.metadata.id']),
            segments=params['gateway.upload.segment']
        )

    else:
        # Get s3 object key prefix
        prefix = s3_gateway2.util.metadata_id.object_key(params['gateway.metadata.parent.id']) \
            if params['gateway.metadata.parent.id'] else None
        if prefix:
            assert prefix[-1] == '/'

        # Create upload for folder.
        new_upload = s3_gateway2.controller.s3.create_new_file_upload(
            region=params['config.region'],
            host=params['config.host'],
            access_key=params['config.access.key'],
            access_key_secret=params['config.access.key.secret'],
            bucket=params['config.bucket'],
            key_prefix=prefix,
            file_name=params['gateway.metadata.name'],
            segments=params['gateway.upload.segment']
        )

    if new_upload is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    # Send new metadata.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(new_upload)
    }


# Cancel upload session.
# DELETE /v2/gateway_upload/<gateway.upload.id>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def delete_gateway_upload(environ, params):
    assert params.get('gateway.upload.id')

    #
    # Execute request.
    #

    deleted = s3_gateway2.controller.s3.delete_upload(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        gateway_upload_id=params['gateway.upload.id'],
    )
    if not deleted:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    return {
        'code': '200',
        'message': 'ok',
    }
