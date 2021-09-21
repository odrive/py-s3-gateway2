import util.handler


def handle(environ):

    #
    # Params.
    #

    params = {
        # From PATH_INFO:
        # /v2/file_thumbnail/<content.id>
        'metadata.content.id': environ['PATH_INFO'][19:] if len(environ['PATH_INFO']) > 19 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_file_thumbnail' if params['metadata.content.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Download icon.
# GET /v2/file_thumbnail/<content.id>
@util.handler.limit_usage
def _get_file_thumbnail(environ, params):
    return {
        'code': '403',
        'message': 'Not available'
    }
