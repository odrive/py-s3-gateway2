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
        # URI /v2/gateway_upload_segment/<gateway.update.id>
        'gateway.upload.id': environ['PATH_INFO'][27:] if len(environ['PATH_INFO']) > 27 else None,
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


# Upload large file segment.
# POST /v2/gateway_upload_segment/<gateway.upload.id>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.load_access_token
@s3_gateway2.util.handler.load_s3_config
@s3_gateway2.util.handler.handle_s3_exception
def _post_gateway_upload(environ, params):
    assert params['gateway.upload.id']

    #
    # Load params.
    #

    params.update({
        'gateway.upload.segment.number': None,
        'gateway.upload.segment.sha256': None,
        'gateway.upload.segment.size': None,
        'gateway.upload.segment.cookie': None,
        'gateway.upload.cookie': None,
    })

    # Load headers.
    if environ.get('HTTP_X_GATEWAY_UPLOAD_SEGMENT'):  # wsgi adds HTTP to the header and converts - to _, so client should use X-GATEWAY-UPLOAD-SEGMENT
        header_params = json.loads(environ['HTTP_X_GATEWAY_UPLOAD_SEGMENT'])
        if header_params:
            params['gateway.upload.segment.number'] = header_params.get('gateway.upload.segment.number')
            params['gateway.upload.segment.sha256'] = header_params.get('gateway.upload.segment.sha256')
            params['gateway.upload.segment.size'] = header_params.get('gateway.upload.segment.size')
            params['gateway.upload.segment.cookie'] = header_params.get('gateway.upload.segment.cookie')
            params['gateway.upload.cookie'] = header_params.get('gateway.upload.cookie')

    #
    # Validate request.
    #

    # Validata segment params.
    if not isinstance(params['gateway.upload.segment.number'], int):
        return {
            'code': '400',
            'message': 'Invalid gateway.upload.segment.number.'
        }
    if not isinstance(params['gateway.upload.segment.sha256'], str):
        return {
            'code': '400',
            'message': 'Invalid gateway.upload.segment.sha256.'
        }
    if not isinstance(params['gateway.upload.segment.size'], int):
        return {
            'code': '400',
            'message': 'Invalid gateway.upload.segment.size.'
        }
    if params['gateway.upload.segment.size'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.upload.segment.size.'
        }
    if params['gateway.upload.segment.cookie'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.upload.segment.cookie.'
        }

    # Validate upload cookie.
    if params['gateway.upload.cookie'] is None:
        return {
            'code': '400',
            'message': 'Missing gateway.upload.cookie.'
        }

    # Check missing input.
    if not environ['wsgi.input']:
        return {
            'code': '400',
            'message': 'Missing wsgi.input'
        }

    #
    # Execute request.
    #

    # Upload segment.
    new_segment = s3_gateway2.controller.s3.upload_segment(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        segment_number=params['gateway.upload.segment.number'],
        segment_size=params['gateway.upload.segment.size'],
        segment_sha256=params['gateway.upload.segment.sha256'],
        gateway_upload_id=params['gateway.upload.id'],
        input_stream=environ['wsgi.input']
    )
    if new_segment is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    # Send new metadata.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(new_segment)
    }
