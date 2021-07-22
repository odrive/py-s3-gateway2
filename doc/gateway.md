# Storage Gateway
odrive integrates with applications through Storage Gateways. 

A Storage Gateway is simply a web server implementing the Gateway API used by odrive to access and synchronize files. 

To integrate your application with odrive, build a custom Storage Gateway and make it accessible to odrive on the Internet.

# Gateway API
The Gateway API specifies how odrive authorizes users and accesses storage. A Storage Gateway implements the Gateway API endpoints by translating them to internal application calls and returning standardized responses. 

## Integration Model
A Storage Gateway enables odrive access to its source system. It
maps its source data into a virtual, hierarchical file system represented by FILE and METADATA resources. 

A Storage Gateway's file system is navigable. Upon authorization, the gateway returns the session root folder as the starting point for browsing.

## API Resources

**AUTH**

AUTH objects represent the gateway's access authorization. AUTH endpoints support:
- Signing in
- Signing out
- Refreshing access tokens

**FILE**

FILE objects represent the binary file data. FILE endpoints support:
- Downloading files
- Downloading thumbnails

**METADATA**

METADATA resource represents the properties of files and folders. METADATA endpoints support:
- Listing folder content
- Getting properties
- Creating folders
- Uploading files
- Updating files
- Moving files or folders
- Rename files or folders
- Delete files or folders

# API Endpoints

# AUTH

## Sign in
```
POST /v2/auth
```
**Request Body JSON**

Property | Description
---------|-------------
`<credential.name>` | Sign-in requirements are specific to each Gateway.

**Response JSON**

Property | Description
---------|------------
`access.token` | Required AUTHORIZATION header for subsequent API requests.
`refresh.token` | Required to refresh expired access tokens.
`root.content.id` | The root folder to start browsing. 

**Response Status**

Status | Description
-------|------------
`200` | OK
`400` | Missing credential
`403` | Invalid credential
`403` | Not allowed
`429` | Rate limited

## Sign out
```
DELETE /v2/auth/<access.token>
```

**Request URL**

Property | Description
---------|-------------
`access.token` | Session to sign out.

**Response Status**

Status | Description
-------|------------
`200` | OK
`404` | access.token not found
`429` | Rate limited

## Refresh expired access token
```
POST /v2/auth
```
**Request Body JSON**

Property | Description
---------|-------------
`refresh.token` | Refresh.token from sign in.

**Response JSON**

Property | Description
---------|------------
`access.token` | Required AUTHORIZATION header for subsequent API requests.
`refresh.token` | Required to refresh expired access tokens.
`root.content.id` | Root folder to start browsing. 

**Response Status**

Status | Description
-------|------------
`200` | OK
`400` | Missing refresh.token
`403` | Not allowed
`429` | Rate limited

# FILE

## Download file
```
GET /v2/file/<content.id>
```

**Request URL**

Property | Description
---------|-------------
`content.id` | File to download.

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token

**Response Body**

The file binary stream.

**Response Status**

Status | Description
-------|------------
`200` | OK
`400` | Not file
`401` | Authorization required
`403` | Not allowed
`404` | Not found
`429` | Rate limited


## Download thumbnail
```
GET /v2/file_thumbnail/<content.id>
```
**Request URL**

Property | Description
---------|-------------
`content.id` | File thumbnail to download.

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token

**Response Body**

The thumbnail binary stream.

**Response Status**

Status | Description
-------|------------
`200` | OK
`401` | Authorization required
`403` | Not allowed
`403` | Not available
`404` | Not found
`429` | Rate limited

# METADATA

## Create new sub folder
```
POST /v2/metadata_folder/<content.id>
```
**Request URL**

Property | Description
---------|-------------
`content.id` | Parent folder

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token

**Body JSON**

Property | Description
---------|-------------
`content.name` | New folder name

**Response JSON**

Property | Description
---------|------------
`content.id` | New folder
`content.type`| `folder`
`content.name`| New folder name

**Response Status**

Status | Description
-------|------------
`200` | OK
`400` | Missing folder name
`401` | Authorization required
`403` | Not allowed
`429` | Rate limited


## Delete content
```
DELETE /v2/metadata/<content.id>
```

**Request URL**

Property | Description
---------|-------------
`content.id` | File or folder to delete

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token

**Response Status**

Status | Description
-------|------------
`200` | OK
`401` | Authorization required
`403` | Not allowed
`404` | Not found
`429` | Rate limited


## Get content metadata
```
GET /v2/metadata/<content.id>
```

**Request URL**

Property | Description
---------|-------------
`content.id` | File or folder

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token

**Response JSON**

Property | Description
---------|------------
`content.id` | File or folder
`content.type`| `folder` or `file`
`content.name`| File or folder name
`file.size`| Total bytes
`file.modified` | Millis since the epoch
`file.etag` | Same etag means the file has not changed

**Response Status**

Status | Description
-------|------------
`200` | OK
`401` | Authorization required
`403` | Not allowed
`404` | Not found
`429` | Rate limited

## List folder content metadata
```
GET /v2/metadata_children/<content.id>?page=
```

**Request URL**

Property | Description
---------|-------------
`content.id` | Folder
`page` | Next page token

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token

**Response Header**

Property | Description
---------|-------------
`page` | Next page token

**Response JSON**

List of metadata properties:

Property | Description
---------|------------
`content.id` | File or folder
`content.type`| `folder` or `file`
`content.name`| File or folder name
`file.size`| Total bytes
`file.modified` | Millis since the epoch
`file.etag` | Same etag means the file has not changed

**Response Status**

Status | Description
-------|------------
`200` | OK
`400` | Not a folder
`401` | Authorization required
`403` | Not allowed
`404` | Not found
`429` | Rate limited

## Move content
```
PATCH /v2/metadata_parent/<content.id>
```

**Request URL**

Property | Description
---------|-------------
`content.id` | File or folder to move

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token

**Body JSON**

Property | Description
---------|-------------
`parent.content.id` | New folder

**Response Status**

Status | Description
-------|------------
`200` | OK
`401` | Authorization required
`403` | Not allowed
`404` | Not found
`429` | Rate limited

## Rename content
```
PATCH /v2/metadata_name/<content.id>
```

**Request URL**

Property | Description
---------|-------------
`content.id` | File or folder to rename

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token

**Body JSON**

Property | Description
---------|-------------
`content.name` | New name

**Response Status**

Status | Description
-------|------------
`200` | OK
`401` | Authorization required
`403` | Not allowed
`404` | Not found
`429` | Rate limited


## Upload new file
```
POST /v2/metadata_file/<content.id>
```

**Request URL**

Property | Description
---------|-------------
`content.id` | Parent folder

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token
`HTTP_X_UPLOAD_JSON` | Special file upload JSON (see below).

**HTTP_X_UPLOAD_JSON Header**

Property | Description
---------|-------------
`content.name` | New file name
`file.modified` | New file modified time (Millis since the epoch)
`file.size` | New file size

**Request Body**

The file binary stream.

**Response Status**

Status | Description
-------|------------
`200` | OK
`401` | Authorization required
`403` | Not allowed
`404` | Not found
`429` | Rate limited

## Update existing file
```
PUT /v2/metadata/<content.id>
```

**Request URL**

Property | Description
---------|-------------
`content.id` | File to update

**Request Header**

Property | Description
---------|-------------
`AUTHORIZATION` | Access token
`HTTP_X_UPLOAD_JSON` | Special file upload JSON (see below).

**HTTP_X_UPLOAD_JSON Header**

Property | Description
---------|-------------
`file.modified` | Updated modified time (Millis since the epoch)
`file.size` | Updated file size

**Request Body**

The file binary stream.

**Response Status**

Status | Description
-------|------------
`200` | OK
`401` | Authorization required
`403` | Not allowed
`404` | Not found
`429` | Rate limited

# Error Handling

Status | Description
-------|------------
`500` | Unexpected exception
`502` | Unexpected error from downstream service
`503` | A downstream service is temporarily unavailable
`504` | No response from downstream service
