import os
import json
import cherrypy
from s3_gateway2 import dispatcher


def main():

    #
    # Load deployment config.
    #

    deployment_config = {
        # Configure web listener.
        's3_gateway2.deployment.server.port': 10087,
        's3_gateway2.deployment.server.thread.pool': 50,

        # Set app data folder.
        's3_gateway2.deployment.data.dir': 'data',
    }

    # Override defaults with config file.
    config_file = os.path.join(os.getcwd(), 'config.json')
    assert os.path.exists(config_file)
    with open(config_file, 'r') as json_file:
        deployment_config.update(json.load(json_file))

    #
    # Setup runtime environment.
    #

    # Create data folder for working files. Must be a writable folder.
    data_dir = os.path.abspath(deployment_config.get('s3_gateway2.deployment.data.dir'))
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    #
    # Configure S3 Gateway2.
    #
    
    s3_gateway2_config = {
        # Enable server request throttling.
        's3_gateway2.util.handler.usage.interval.seconds': 10,
        's3_gateway2.util.handler.usage.count.max': 100,

        # Enable server request logging.
        's3_gateway2.wsgi.log.enable': True,
        's3_gateway2.wsgi.log.file': os.path.join(data_dir, 'server.log'),
        
        # Configure s3 api integration.
        's3_gateway2.util.s3.log.enable': True,
        's3_gateway2.util.s3.log.file': os.path.join(data_dir, 's3.log'),
        's3_gateway2.util.s3.request.timeout': 15,
        's3_gateway2.controller.datastore.dir': os.path.join(data_dir, 'datastore')
    }    
    
    # Ensure folder ready.
    if not os.path.exists(s3_gateway2_config['s3_gateway2.controller.datastore.dir']):
        os.makedirs(s3_gateway2_config['s3_gateway2.controller.datastore.dir'])
    
    # Load config.
    dispatcher.update_config(s3_gateway2_config)
    
    #
    # launch cherrypy server
    #
    
    # mount our wsgi app to cherry root
    cherrypy.tree.graft(dispatcher.dispatch, '/')
    
    # configure cherrypy
    cherrypy.config.update({
        'server.socket_port': deployment_config['s3_gateway2.deployment.server.port'],
        'server.socket_host': '127.0.0.1',
        'server.thread_pool': deployment_config['s3_gateway2.deployment.server.thread.pool'],
        # remove any limit on request body size; default is 100MB; Use 2147483647 for 2GB
        'server.max_request_body_size': 0,
    })

    # run
    cherrypy.engine.signals.subscribe()
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == '__main__':
    main()
