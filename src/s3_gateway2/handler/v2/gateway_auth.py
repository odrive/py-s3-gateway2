import json
import random
import string
import s3_gateway2.util.s3
import s3_gateway2.util.handler
import s3_gateway2.controller.datastore


def handle(environ):

    #
    # Params.
    #

    params = {
        # From PATH_INFO
        # /v2/gateway_auth/<gateway.auth.access.token>
        'gateway.auth.access.token': environ['PATH_INFO'][17:] if len(environ['PATH_INFO']) > 17 else None,
    }

    #
    # Validate.
    #

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_gateway_auth' if params['gateway.auth.access.token'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '400',
        'message': 'Not found.'
    }


# Sign in.
# POST /v2/gateway_auth
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
@s3_gateway2.util.handler.handle_s3_exception
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
    if not s3_gateway2.util.s3.check_bucket_exists(
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
        'config.root.metadata.id': '',
    }
    assert s3_gateway2.controller.datastore.put(access_token, config, 'registration')
    response = {
        'gateway.auth.access.token': access_token,
        'gateway.auth.refresh.token': None,
        'gateway.auth.metadata.id': config['config.root.metadata.id'],
        'gateway.auth.id': None,
        'gateway.upload.segment.size': _config['s3_gateway2.handler.v2.gateway_auth.upload.segment.size'],
    }
    return {
        'code': '200',
        'message': 'OK',
        'contentType': 'application/json',
        'content': json.dumps(response),
    }


# Sign out.
# DELETE /v2/gateway_auth/<gateway.auth.access.token>
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
@s3_gateway2.util.handler.handle_requests_exception
def _delete_gateway_auth(environ, params):
    assert params.get('gateway.auth.access.token')

    #
    # params
    #

    params.update({
        # datastore
        'registration': None
    })

    # load datastore
    params['registration'] = s3_gateway2.controller.datastore.get(params['gateway.auth.access.token'], 'registration')

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
    s3_gateway2.controller.datastore.delete(params['gateway.auth.access.token'], 'registration')
    return {
        'code': '200',
        'message': 'OK'
    }


#
# Config
#

def update_config(config):
    # Load relevant configurations.
    for key in _config.keys():
        _config[key] = config[key]


_config = {
    's3_gateway2.handler.v2.gateway_auth.upload.segment.size': 100 * 1024 * 1024,
}
