import json
import s3_gateway2.util.handler


def handle(environ):

    #
    # Load params.
    #

    params = {
        # From PATH_INFO: /v2/gateway_auth_method
        'gateway.metadata.id': environ['PATH_INFO'][23:] if len(environ['PATH_INFO']) > 23 else None,
    }

    #
    # Delegate.
    #

    delegate_func = '_{}{}'.format(
        environ['REQUEST_METHOD'].lower(),
        '_gateway_auth_method' if params['gateway.metadata.id'] else ''
    )
    if delegate_func in globals():
        return eval(delegate_func)(environ, params)

    # Unknown.
    return {
        'code': '400',
        'message': 'Not found.'
    }


# Get supported gateway auth method.
# GET /v2/gateway_auth_method
@s3_gateway2.util.handler.handle_unexpected_exception
@s3_gateway2.util.handler.limit_usage
def _get(environ, params):
    return {
        'code': '200',
        'message': 'OK',
        'contentType': 'application/json',
        'content': json.dumps({
            'gateway.auth.method': 'form',
            'gateway.auth.form': [
                {
                    'gateway.auth.form.input.field.name': 'host',
                    'gateway.auth.form.input.field.prompt': 'What is the S3 host server URL?',
                    'gateway.auth.form.input.field.required': True,
                    'gateway.auth.form.input.field.order': 1,
                },
                {
                    'gateway.auth.form.input.field.name': 'region',
                    'gateway.auth.form.input.field.prompt': 'What is the AWS region?',
                    'gateway.auth.form.input.field.required': True,
                    'gateway.auth.form.input.field.order': 2,
                },
                {
                    'gateway.auth.form.input.field.name': 'bucket',
                    'gateway.auth.form.input.field.prompt': 'What is the name of the bucket?',
                    'gateway.auth.form.input.field.required': True,
                    'gateway.auth.form.input.field.order': 3,
                },
                {
                    'gateway.auth.form.input.field.name': 'key',
                    'gateway.auth.form.input.field.prompt': 'Enter the access key.',
                    'gateway.auth.form.input.field.required': True,
                    'gateway.auth.form.input.field.order': 4,
                },
                {
                    'gateway.auth.form.input.field.name': 'secret',
                    'gateway.auth.form.input.field.prompt': 'Enter the access key secret.',
                    'gateway.auth.form.input.field.required': True,
                    'gateway.auth.form.input.field.order': 5,
                }

            ]
        })
    }
