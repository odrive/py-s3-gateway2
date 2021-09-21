import util.handler


def handle(environ):

    #
    # Params.
    #

    params = {
        # From PATH_INFO:
        # /v2/gateway_file_thumbnail/<gateway.metadata.id>
        'gateway.metadata.id': environ['PATH_INFO'][27:] if len(environ['PATH_INFO']) > 27 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_gateway_file_thumbnail' if params['gateway.metadata.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Download icon.
# GET /v2/gateway_file_thumbnail/<gateway.metadata.id>
@util.handler.limit_usage
def _get_gateway_file_thumbnail(environ, params):
    return {
        'code': '403',
        'message': 'Not available'
    }
