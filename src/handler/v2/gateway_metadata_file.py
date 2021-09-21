import json
import util.handler
import util.metadata_id
import controller.s3
import requests_toolbelt


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/metadata_file/<content.id>
        'metadata.content.id': environ['PATH_INFO'][18:] if len(environ['PATH_INFO']) > 18 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_metadata_file' if params['metadata.content.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Upload file to root.
# POST /v2/metadata_file
def _post(environ, params):
    return _post_metadata_file(environ, params)


# Upload file to folder.
# POST /v2/metadata_file/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _post_metadata_file(environ, params):

    #
    # Load params.
    #

    params.update({
        'metadata.content.name': None,
        'metadata.content.modified': None,
        'metadata.file.size': None,
    })

    # Load headers.
    if environ.get('HTTP_X_GATEWAY_UPLOAD'):  # wsgi adds HTTP to the header, so client should use X_UPLOAD_JSON
        header_params = json.loads(environ['HTTP_X_GATEWAY_UPLOAD'])
        if header_params:
            if header_params.get('metadata.content.name'):
                params['metadata.content.name'] = header_params['metadata.content.name'].encode('ISO-8859-1').decode('unicode-escape')
            params['metadata.file.size'] = header_params.get('metadata.file.size')
            params['metadata.content.modified'] = header_params.get('metadata.content.modified')

    #
    # Validate request.
    #

    # Validate create file params.
    if params['metadata.file.size'] is None:
        return {
            'code': '400',
            'message': 'Missing file.size.'
        }
    if not isinstance(params['metadata.file.size'], int):
        return {
            'code': '400',
            'message': 'Invalid size.'
        }
    if params['metadata.content.modified'] is None:
        return {
            'code': '400',
            'message': 'Missing content.modified.'
        }
    if not isinstance(params['metadata.content.modified'], int):
        return {
            'code': '400',
            'message': 'Invalid content.modified.'
        }

    #
    # Execute request.
    #

    prefix = util.metadata_id.object_key(params['metadata.content.id']) if params['metadata.content.id'] else None
    if prefix:
        assert prefix[-1] == '/'
    new_content = controller.s3.create_file(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        key_prefix=prefix,
        file_name=params['metadata.content.name'],
        size=params['metadata.file.size'],
        modified=params['metadata.content.modified'],
        data=environ['wsgi.input']
    )
    if new_content is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    # Send new content.
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(new_content)
    }


def _file_stream_generator(upload, limit):
    uploaded_bytes = 0
    chunk_size = 1024 * 1024 * 2  # 2MB chunk/buffer size
    while True:
        if limit <= uploaded_bytes:
            break
        out = upload.read(chunk_size)
        if not out:
            break
        uploaded_bytes += chunk_size
        yield out