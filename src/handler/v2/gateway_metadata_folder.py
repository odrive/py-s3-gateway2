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
        # URI /v2/metadata_folder/<content.id>
        'metadata.content.id': environ['PATH_INFO'][20:] if len(environ['PATH_INFO']) > 20 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_metadata_folder' if params['metadata.content.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Create root sub folder.
# POST /v2/metadata_folder
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _post(environ, params):
    #
    # Load.
    #

    params.update({
        'metadata.content.name': None,
        'metadata.content.modified': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['metadata.content.name'] = body.get('metadata.content.name')
    params['metadata.content.modified'] = body.get('metadata.content.modified')

    #
    # Validate.
    #

    # Validate name.
    if params['metadata.content.name'] is None:
        return {
            'code': '400',
            'message': 'Missing folder.name.'
        }

    # S3 does not support modified. So igmore.
    # if params['metadata.content.modified'] is None:
    #     return {
    #         'code': '400',
    #         'message': 'Missing content.modified.'
    #     }
    # if not isinstance(params['metadata.content.modified'], int):
    #     return {
    #         'code': '400',
    #         'message': 'Invalid content.modified.'
    #     }

    #
    # Execute.
    #

    new_folder = controller.s3.create_folder(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        key_prefix=None,
        folder_name=params['metadata.content.name']
    )
    if new_folder is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(new_folder)
    }


# Create sub folder.
# POST /v2/metadata_folder/<content.id>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.load_access_token
@util.handler.load_s3_config
@util.handler.handle_s3_exception
def _post_metadata_folder(environ, params):

    #
    # Load.
    #

    params.update({
        'metadata.content.name': None,
        'metadata.content.modified': None,
    })

    # Load body.
    body = json.load(environ['wsgi.input'])
    params['metadata.content.name'] = body.get('metadata.content.name')
    # params['metadata.content.modified'] = body.get('metadata.content.modified')

    #
    # Validate.
    #

    # Validate name.
    if params['metadata.content.name'] is None:
        return {
            'code': '400',
            'message': 'Missing folder.name.'
        }

    # S3 does not support modified. So igmore.
    # if params['metadata.content.modified'] is None:
    #     return {
    #         'code': '400',
    #         'message': 'Missing content.modified.'
    #     }
    # if not isinstance(params['metadata.content.modified'], int):
    #     return {
    #         'code': '400',
    #         'message': 'Invalid content.modified.'
    #     }

    #
    # Execute.
    #

    new_folder = controller.s3.create_folder(
        region=params['config.region'],
        host=params['config.host'],
        access_key=params['config.access.key'],
        access_key_secret=params['config.access.key.secret'],
        bucket=params['config.bucket'],
        key_prefix=util.metadata_id.object_key(params['metadata.content.id']),
        folder_name=params['metadata.content.name']
    )
    if new_folder is None:
        return {
            'code': '403',
            'message': 'Not allowed.'
        }

    return {
        'code': '200',
        'message': 'ok',
        'contentType': 'application/json',
        'content': json.dumps(new_folder)
    }
