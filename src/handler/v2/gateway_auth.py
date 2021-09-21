import json
import random
import string
import util.s3
import util.handler
import controller.datastore


def handle(environ):

    #
    # Params.
    #

    params = {
        # From PATH_INFO
        # /v2/auth/<access.token>
        'auth.access.token': environ['PATH_INFO'][9:] if len(environ['PATH_INFO']) > 9 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_auth' if params['auth.access.token'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '404',
        'message': 'Not found.'
    }


# Sign in.
# POST /v2/auth
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
@util.handler.handle_s3_exception
def _post(environ, params):

    #
    # Params.
    #

    params.update({
        # From body.
        'host': None,
        'region': None,
        'bucket': None,
        'key': None,
        'secret': None,
    })

    # Load body.
    params.update(json.load(environ['wsgi.input']) if environ.get('wsgi.input') else {})

    #
    # Validate.
    #

    # check params
    if params['host'] is None:
        return {
            'code': '400',
            'message': 'Missing host'
        }
    if params['bucket'] is None:
        return {
            'code': '400',
            'message': 'Missing bucket'
        }
    if params['region'] is None:
        return {
            'code': '400',
            'message': 'Missing region'
        }
    if params['key'] is None:
        return {
            'code': '400',
            'message': 'Missing key'
        }
    if params['secret'] is None:
        return {
            'code': '400',
            'message': 'Missing secret'
        }

    #
    # Execute
    #

    # Check credentials.
    if not util.s3.check_bucket_exists(
        region=params['region'],
        host=params['host'],
        access_key=params['key'],
        access_key_secret=params['secret'],
        bucket=params['bucket']
    ):
        # Invalid credentials.
        return {
            'code': '403',
            'message': 'Invalid credentials'
        }

    # Register credentials.
    access_token = ''.join(
        random.SystemRandom().choice(
            string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(32)
    )
    config = {
        'config.host': params['host'],
        'config.region': params['region'],
        'config.bucket': params['bucket'],
        'config.access.key': params['key'],
        'config.access.key.secret': params['secret'],
        'config.root.content.id': '',
    }
    assert controller.datastore.put(access_token, config, 'registration')
    response = {
        'auth.access.token': access_token,
        'auth.refresh.token': None,
        'auth.metadata.content.id': config['config.root.content.id']
    }
    return {
        'code': '200',
        'message': 'OK',
        'contentType': 'application/json',
        'content': json.dumps(response),
    }


# Sign out.
# DELETE /v2/auth/<access.token>
@util.handler.handle_unexpected_exception
@util.handler.limit_usage
@util.handler.handle_requests_exception
def _delete_auth(environ, params):
    assert params.get('auth.access.token')

    #
    # params
    #

    params.update({
        # datastore
        'registration': None
    })

    # load datastore
    params['registration'] = controller.datastore.get(params['auth.access.token'], 'registration')

    #
    # validate
    #

    # check registration
    if params['registration'] is None:
        return {
            'code': '200',
            'message': 'OK'
        }

    #
    # execute
    #

    # delete registration
    controller.datastore.delete(params['auth.access.token'], 'registration')
    return {
        'code': '200',
        'message': 'OK'
    }

