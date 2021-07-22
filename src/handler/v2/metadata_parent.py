import json
import util.handler
import util.content_id
import controller.s3


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/metadata_parent/<content.id>
        'content.id': environ['PATH_INFO'][20:] if len(environ['PATH_INFO']) > 20 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_metadata_parent' if params['content.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Move file or folder.
# PATCH /v2/metadata_parent/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _patch_metadata_parent(environ, params):
    assert params.get('content.id')

    #
    # Load.
    #

    params.update({
        'parent.content.id': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['parent.content.id'] = body.get('parent.content.id')

    #
    # Validate.
    #

    # Check new parent.
    if params['parent.content.id'] is None:
        return {
            'code': '400',
            'message': 'Missing parent.content.id.'
        }

    # Check source.
    if util.content_id.object_key(params['content.id'])[-1] == '/':
        # Not allowed to move folder.
        return {
            'code': '403',
            'message': 'Not allowed to move folder.'
        }

    #
    # Execute.
    #

    moved_content = controller.s3.move(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        object_key=util.content_id.object_key(params['content.id']),
        new_prefix=util.content_id.object_key(params['parent.content.id'])
    )
    if moved_content is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(moved_content)
    }
