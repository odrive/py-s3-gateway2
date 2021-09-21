import json
import util.handler
import util.metadata_id
import controller.s3


def handle(environ):

    #
    # Load.
    #

    # PATH_INFO
    params = {
        # URI /v2/metadata_content_parent/<content.id>
        'metadata.content.id': environ['PATH_INFO'][28:] if len(environ['PATH_INFO']) > 28 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_metadata_content_parent' if params['metadata.content.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Move file or folder.
# PATCH /v2/metadata_content_parent/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _patch_metadata_content_parent(environ, params):
    assert params.get('metadata.content.id')

    #
    # Load.
    #

    params.update({
        'new.metadata.content.parent.id': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['new.metadata.content.parent.id'] = body.get('new.metadata.content.parent.id')

    #
    # Validate.
    #

    # Check new parent.
    if params['new.metadata.content.parent.id'] is None:
        return {
            'code': '400',
            'message': 'Missing new.metadata.content.parent.id.'
        }

    # Check source.
    if util.metadata_id.object_key(params['metadata.content.id'])[-1] == '/':
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
        object_key=util.metadata_id.object_key(params['metadata.content.id']),
        new_prefix=util.metadata_id.object_key(params['new.metadata.content.parent.id'])
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
