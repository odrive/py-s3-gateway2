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
  "s3_gateway2.deployment.server.port": 10080,
  "s3_gateway2.deployment.server.thread.pool": 50,
  "s3_gateway2.deployment.data.dir": "data"
}
```

The default configuration uses the current working directory for the data directory containing logs and session files. 

If you want to change the runtime settings, update the properties in `config.json` and restart.

Property | Description
---|---
`s3_gateway2.deployment.server.port` | Gateway server port.
`s3_gateway2.deployment.server.thread.pool` | Available request worker threads.
`s3_gateway2.deployment.data.dir` | Gateway log and session directory.


# Launch

Start the gateway from the project bin directory.

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

*Download link and screen shots coming soon. Please contact odrive to recieve early access.*

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
