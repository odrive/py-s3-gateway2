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
        # URI /v2/metadata_content_name/<content.id>
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
        '_metadata_content_name' if params['metadata.content.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Rename file or folder.
# PATCH /v2/metadata_content_name/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _patch_metadata_content_name(environ, params):
    assert params.get('metadata.content.id')

    #
    # Load.
    #

    params.update({
        'new.metadata.content.name': None,
        'old.metadata.content.name': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['new.metadata.content.name'] = body.get('new.metadata.content.name')
    params['old.metadata.content.name'] = body.get('old.metadata.content.name')

    #
    # Validate.
    #

    # Validate name.
    if params['new.metadata.content.name'] is None:
        return {
            'code': '400',
            'message': 'Missing new.metadata.content.name'
        }

    #
    # Execute.
    #

    renamed_content = controller.s3.rename(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        object_key=util.content_id.object_key(params['metadata.content.id']),
        new_name=params['new.metadata.content.name'],
    )
    if renamed_content is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }
    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(renamed_content)
    }
