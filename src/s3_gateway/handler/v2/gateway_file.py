import s3_gateway.util.handler
import s3_gateway.util.metadata_id
import s3_gateway.controller.s3


def handle(environ):

    #
    # Load params.
    #

    params = {
        # From PATH_INFO
        # /v1/gateway_file/<gateway.metadata.id>
        'gateway.metadata.id': environ['PATH_INFO'][17:] if len(environ['PATH_INFO']) > 17 else None,
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
        'code': '400',
        'message': 'Invalid Endpoint'
    }


# Download file.
# GET /v2/gateway_file/<gateway.metadata.id>
@s3_gateway.util.handler.handle_unexpected_exception
@s3_gateway.util.handler.limit_usage
@s3_gateway.util.handler.handle_requests_exception
@s3_gateway.util.handler.load_access_token
@s3_gateway.util.handler.load_s3_config
@s3_gateway.util.handler.handle_s3_exception
def _get_gateway_metadata(environ, params):
    assert params.get('gateway.metadata.id')

    #
    # Validate.
    #

    object_key = s3_gateway.util.metadata_id.object_key(params['gateway.metadata.id'])
    if object_key[-1] == '/':
        # Not file.
        return {
            'code': '400',
            'message': 'Not file.'
        }

    #
    # Execute.
    #

    file_iterator = s3_gateway.controller.s3.iter_file(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        object_key=object_key
    )
    if file_iterator is None:
        # Not found.
        return {
            'code': '404',
            'message': 'Not found.'
        }
    return {
        'code': '200',
        'message': 'OK',
        'contentType': 'application/octet-stream',
        'contentIterator': file_iterator,
    }
