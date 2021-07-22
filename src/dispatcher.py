import wsgi
import util.s3
import util.handler
import controller.datastore


def dispatch(environ, start_response):
    return wsgi.dispatch(environ, start_response)


def update_config(properties):

    #
    # framework
    #

    wsgi.update_config({
        'log.enable': properties.get('wsgi.log.enable'),
        'log.path': properties.get('wsgi.log.path'),
    })

    #
    # handler
    #

    #
    # controller
    #

    controller.datastore.update_config({
        'path': properties['controller.datastore.path']
    })

    #
    # util
    #

    util.handler.update_config({
        "usage.interval.seconds": properties.get('util.handler.usage.interval.seconds'),
        "usage.count.max": properties.get('util.handler.usage.count.max'),
    })

    util.s3.update_config({
        'log.file.path': properties.get('util.s3.log.file.path'),
        'log.enable': properties.get('util.s3.log.enable'),
    })
