# S3 Gateway
odrive integrates with applications through Storage Gateways.

A Storage Gateway is simply a web server implementing the [Gateway API](https://github.com/odrive/gateway-api) used by odrive to access and synchronize files.

The S3 Gateway is a reference implementation of the Gateway API. It provides odrive access to Amazon S3 buckets. 

# Setup

- Clone repo
- Install Python3
- Install latest PIP
- Create Python virtual environment:

*Windows*
```
setup/setup_win.ps1
```

*Mac or Linux*
```
setup/setup_mac.sh
```

# Configure Gateway
`/bin/config.json` defines the runtime configuration.
```
# /bin/config.json
{
  "controller.datastore.path": "datastore",
  "util.handler.usage.interval.seconds": 1000,
  "util.handler.usage.count.max": 60,
  "util.s3.log.enable": true,
  "util.s3.log.file.path": "s3.log"
  "wsgi.log.enable": true,
  "wsgi.log.path": "server.log",
}
```

The default configuration enables logging and uses the current working directory for logs and session files. 

If you want to change the runtime settings, update the properties in `config.json` and restart.

Property | Description
---|---
`controller.datastore.path` | Where to store session files.
`util.handler.usage.interval.seconds` | Sampling period for limiting usage.
`util.handler.usage.count.max` | Maximum requests within usage interface. Requests beyond the max return 429 status code.
`util.s3.log.enable` | Set `true` to log every s3 request.
`util.s3.log.file.path` | Relative or absolute path to s3 log file.
`wsgi.log.enable` | Set `true` to log every server request.
`wsgi.log.path` | Relative or absolute path to server log. 

# Configure cherrypy

S3 Gateway is a WSGI application running in cherrypy. `/bin/run.py` defines the cherrypy configuration.

```
# configure cherrypy
cherrypy.config.update({
    'server.socket_port': 9083,
    'server.socket_host': '127.0.0.1',
    'server.thread_pool': 30,
    # remove any limit on request body size; default is 100MB; Use 2147483647 for 2GB
    'server.max_request_body_size': 0,
})
```

Modify `/bin/run.py` to change the cherrypy configuration. For example, update the `server.socket_port` property to change the server port number.


# Launch

Start the web server from the project bin directory.

*Windows*
```
cd bin
python run.py
```

*Mac or Linux*
```
cd bin
python3 run.py
```

# Connect

## Gateway Shell

Access S3 Gateway from the command line with [Gateway Shell](https://github.com/odrive/gateway-api/blob/main/gateway-shell.md). 

```
% gws.exe
gws> gateway list

No authorizations found.
gws> gateway authorize demo http://localhost:9084 '{"region": "us-east-1", "host": "s3.us-west-1.wasabisys.com", "bucket": "gateway-demo", "key": "xxx", "secret": "xxx"}'

Success! Gateway authorized as demo.
gws> gateway list
demo
gws> 
```

## Gateway Sync

Access S3 Gateway directly on your computer with Gateway Sync.

*Download link and screen shots coming soon*

## Gateway API

Access S3 Gateway programmatically with the [Gateway API](https://github.com/odrive/gateway-api). 

All gateways implement the same API except for authorization. Use the S3 Gateway /v2/auth endpoint to sign in and then use the Gateway API to browse and access files in the S3 bucket.

### Signing into S3 Gateway
```
POST /v2/gateway_auth
```
**Request Body JSON**

Property | Description
---------|-------------
`region` | S3 region
`host` | s3 host
`bucket` | s3 bucket to connect
`key` | S3 access key
`secret` | S3 access key secret

**Response JSON**

Property | Description
---------|------------
`gateway.auth.access.token` | Required AUTHORIZATION header for subsequent API requests. Does not expire.
`gateway.auth.metadata.id` | `''` Session root folder ID is an empty string.

**Response Status**

Status | Description
-------|------------
`200` | OK
`400` | Missing region
`400` | Missing host
`400` | Missing bucket
`400` | Missing key
`400` | Missing secret
`403` | Invalid credential
`429` | Rate limited
