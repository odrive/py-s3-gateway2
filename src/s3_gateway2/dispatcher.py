import s3_gateway2.util.s3
import s3_gateway2.util.handler
import s3_gateway2.controller.datastore
import s3_gateway2.wsgi
import s3_gateway2.handler.v2.gateway_auth


def dispatch(environ, start_response):
    return s3_gateway2.wsgi.dispatch(environ, start_response)


def update_config(properties):

    s3_gateway2.wsgi.update_config(properties)
    s3_gateway2.controller.datastore.update_config(properties)
    s3_gateway2.util.handler.update_config(properties)
    s3_gateway2.util.s3.update_config(properties)
    s3_gateway2.handler.v2.gateway_auth.update_config(properties)
