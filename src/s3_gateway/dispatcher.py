import s3_gateway.util.s3
import s3_gateway.util.handler
import s3_gateway.controller.datastore
import s3_gateway.wsgi


def dispatch(environ, start_response):
    return s3_gateway.wsgi.dispatch(environ, start_response)


def update_config(properties):

    #
    # framework
    #

    s3_gateway.wsgi.update_config({
        'log.enable': properties.get('s3_gateway.wsgi.log.enable'),
        'log.path': properties.get('s3_gateway.wsgi.log.path'),
    })

    #
    # handler
    #

    #
    # controller
    #

    s3_gateway.controller.datastore.update_config({
        'path': properties['s3_gateway.controller.datastore.path']
    })

    #
    # util
    #

    s3_gateway.util.handler.update_config({
        "usage.interval.seconds": properties.get('s3_gateway.util.handler.usage.interval.seconds'),
        "usage.count.max": properties.get('s3_gateway.util.handler.usage.count.max'),
    })

    s3_gateway.util.s3.update_config({
        'log.file.path': properties.get('s3_gateway.util.s3.log.file.path'),
        'log.enable': properties.get('s3_gateway.util.s3.log.enable'),
    })
