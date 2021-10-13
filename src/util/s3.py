import logging
import logging.handlers
import time
import os.path
import base64
import requests
import datetime
import hmac
import hashlib
import urllib.request
import urllib.parse
import urllib.error
import xmltodict
from xml.sax.saxutils import escape
from collections import OrderedDict
from datetime import datetime


def check_bucket_exists(region, host, access_key, access_key_secret, bucket):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket

    response = _send_head_bucket(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket
    )

    # handle found
    if response.status_code == 200:
        return True

    # handle now allowed
    if response.status_code == 403:
        return None

    # handle not found
    if response.status_code == 404:
        return False

    # handle unexpected
    raise S3Exception(response)


def create(region, host, access_key, access_key_secret, bucket, object_key,
           content_length, data, content_md5=None):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key
    assert content_length is not None
    assert data or content_length == 0

    response = _send_put_bucket_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key,
        content_length=content_length,
        data=data,
        content_md5=content_md5
    )

    # handle bucket not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 200:
        return response.headers

    # handle unexpected
    raise S3Exception(response)


def create_multipart_upload(region, host, access_key, access_key_secret, bucket, object_key):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key

    response = _send_create_multipart(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    # handle bucket not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 200:
        return response.content

    # handle unexpected
    raise S3Exception(response)


def upload_part(region, host, access_key, access_key_secret, bucket, object_key,
                content_length, file_like_object, part_number, upload_id, content_md5=None):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key
    assert content_length is not None
    assert file_like_object or content_length == 0
    assert part_number
    assert upload_id

    response = _send_upload_part(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key,
        content_length=content_length,
        file_like_object=file_like_object,
        part_number=part_number,
        upload_id=upload_id,
        content_md5=content_md5
    )

    # handle bucket not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 200:
        return response.headers

    # handle unexpected
    raise S3Exception(response)


def complete_multipart_upload(region, host, access_key, access_key_secret, bucket, object_key,
                              upload_id, uploaded_parts):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key
    assert upload_id

    final_parts = []
    for part, etag in enumerate(uploaded_parts):
        final_parts.append('<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>'.format(part + 1, etag))
    data = '<CompleteMultipartUpload>{}</CompleteMultipartUpload>'.format(''.join(final_parts)).encode('utf-8')

    response = _send_complete_multipart(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key,
        upload_id=upload_id,
        data=data
    )

    # handle bucket not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 200:
        return response.content

    # handle unexpected
    raise S3Exception(response)


def abort_multipart_upload(region, host, access_key, access_key_secret, bucket, object_key, upload_id):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key
    assert upload_id

    response = _send_abort_multipart(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key,
        upload_id=upload_id
    )

    # handle bucket not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 204:
        return response.headers

    # handle unexpected
    raise S3Exception(response)


def copy_object(region, host, access_key, access_key_secret, from_bucket, from_object, to_bucket, to_object):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert from_bucket
    assert from_object
    assert to_bucket
    assert to_object

    response = _send_put_bucket_object_copy(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        from_bucket=from_bucket,
        from_object=from_object,
        to_bucket=to_bucket,
        to_object=to_object
    )

    # handle not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 200:
        return xmltodict.parse(response.content)

    # handle unexpected
    raise S3Exception(response)


def delete(region, host, access_key, access_key_secret, bucket, object_key):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key

    response = _send_delete_bucket_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    # handle not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 204:
        return response.headers

    # handle unexpected
    raise S3Exception(response)


def delete_multi(region, host, access_key, access_key_secret, bucket, object_keys):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_keys

    response = _send_post_bucket_delete(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_keys=object_keys
    )

    # handle not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 200:
        return response.content

    # handle unexpected
    raise S3Exception(response)


def get_data_iterator(region, host, access_key, access_key_secret, bucket, object_key, chunk_size=1024 * 1024):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket

    response = _send_get_bucket_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    # handle not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 200:
        return response.iter_content(chunk_size)

    # handle unexpected
    raise S3Exception(response)


def get_object(region, host, access_key, access_key_secret, bucket, object_key):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket

    response = head_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    # handle not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None

    # handle ok
    if response.status_code == 200:
        # convert Last-Modified to ISO 8601 format with microseconds and ending 'Z' to match amazon list_objects call
        response.headers['Last-Modified'] = f"{datetime.strptime(response.headers.get('Last-Modified'), '%a, %d %b %Y %H:%M:%S %Z').isoformat()}.000Z"

        # return metadata in response header
        return response.headers

    # handle unexpected
    raise S3Exception(response)


def list_objects(region, host, access_key, access_key_secret,
                 bucket, prefix=None, delimiter=None, max_items=None, continuation_token=None):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket

    response = _send_get_bucket_query(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_items,
        continuation_token=continuation_token
    )

    # handle not found
    if response.status_code == 404:
        return None

    # handle not allowed
    if response.status_code == 403:
        return None
    if response.status_code == 301:
        # Invalid bucket name.
        return None

    # handle ok
    if response.status_code == 200:
        return response.content

    # handle unexpected
    raise S3Exception(response)


class S3Exception(Exception):
    def __init__(self, http_response):
        self.http_response = http_response

    def __str__(self):
        return str(self.http_response.status_code)


#
# endpoint wrapper
# https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html
#

def log_http_request(send_func):
    def wrapper(*args, **kwargs):

        if _config.get('log.enable') is False:
            # skip logging
            return send_func(*args, **kwargs)

        # make http call
        start_time = time.time()
        try:
            response = send_func(*args, **kwargs)

        except requests.Timeout as e:

            # Log request timeout
            end_time = time.time()
            if _config['log.file.path']:
                logging.getLogger(__name__).info(
                    '{:.3f} [{}] {} {} {}'.format(
                        end_time - start_time,
                        'ERROR',
                        e.request.method,
                        e.request.url,
                        'Request timeout.'
                    )
                )
            raise

        except requests.ConnectionError as e:

            # Log connection error
            end_time = time.time()
            if _config['log.file.path']:
                logging.getLogger(__name__).info(
                    '{:.3f} [{}] {} {} {}'.format(
                        end_time - start_time,
                        'ERROR',
                        e.request.method,
                        e.request.url,
                        'Connection connect.'
                    )
                )
            raise

        # log http response
        end_time = time.time()
        if _config['log.file.path']:
            logging.getLogger(__name__).info(
                '{:.3f} [{}] {} {} {}'.format(
                    end_time - start_time,
                    response.status_code,
                    response.request.method,
                    response.request.url,
                    response.reason
                )
            )

        return response

    return wrapper


# HEAD /<bucket>/
def _send_head_bucket(region, host, access_key, access_key_secret, bucket):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='HEAD', 
        uri='/{}'.format(bucket)
    )


# GET /<bucket>?<query>
def _send_get_bucket_query(region, host, access_key, access_key_secret,
                           bucket, prefix=None, delimiter=None, max_keys=None, continuation_token=None):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))

    # load calculated
    query_params = {
        'list-type': '2',  # use version two
    }
    if prefix:
        query_params['prefix'] = prefix
    if continuation_token:
        query_params['continuation-token'] = continuation_token
    if max_keys:
        query_params['max-keys'] = max_keys
    if delimiter:
        query_params['delimiter'] = delimiter

    #
    # execute
    #

    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='GET', 
        uri='/{}'.format(bucket),
        query_params=query_params
    )


# POST /<bucket>/delete
def _send_post_bucket_delete(region, host, access_key, access_key_secret, bucket, object_keys):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))

    # format object keys to data
    object_tags = []
    for object_key in object_keys:
        object_tags.append('<Object><Key>{}</Key></Object>'.format(escape(object_key)))
    data = '<?xml version="1.0" encoding="UTF-8"?><Delete>{}</Delete>'.format(''.join(object_tags)).encode('utf-8')

    # add headers:
    headers = dict()

    # calculate Content-Length
    headers['Content-Length'] = str(len(data))

    # calculate Content-MD5: base64 encoded MD5 as UTF-8 string
    headers['Content-MD5'] = base64.b64encode(hashlib.md5(data).digest()).decode('utf-8')

    # must add delete to query
    params = {
        'delete': ''
    }

    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='POST', 
        uri='/{}'.format(bucket), 
        query_params=params, 
        data=data, 
        headers=headers
    )


# HEAD /<bucket>/<object-name>
def head_object(region, host, access_key, access_key_secret, bucket, object_key):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    object_key = urllib.parse.quote(object_key.encode('utf-8'))

    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='HEAD', 
        uri='/{}/{}'.format(bucket, object_key)
    )


# GET /<bucket>/<object-name>
def _send_get_bucket_object(region, host, access_key, access_key_secret, bucket, object_key):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    object_key = urllib.parse.quote(object_key.encode('utf-8'))

    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='GET', 
        uri='/{}/{}'.format(bucket, object_key), 
        stream=True
    )


# DELETE /<bucket>/<object-name>
def _send_delete_bucket_object(region, host, access_key, access_key_secret, bucket, object_key):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    object_key = urllib.parse.quote(object_key.encode('utf-8'))

    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='DELETE', 
        uri='/{}/{}'.format(bucket, object_key)
    )


# PUT /<bucket>/<object-name>
def _send_put_bucket_object_copy(region, host, access_key, access_key_secret,
                                 from_bucket, from_object, to_bucket, to_object):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html

    from_bucket = urllib.parse.quote(from_bucket.encode('utf-8'))
    from_object = urllib.parse.quote(from_object.encode('utf-8'))
    to_bucket = urllib.parse.quote(to_bucket.encode('utf-8'))
    to_object = urllib.parse.quote(to_object.encode('utf-8'))

    headers = {
        'x-amz-copy-source': '/{}/{}'.format(from_bucket, from_object)
    }
    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='PUT', 
        uri='/{}/{}'.format(to_bucket, to_object), 
        headers=headers
    )


# PUT /<bucket>/<object-name>
def _send_put_bucket_object(region, host, access_key, access_key_secret,
                            bucket, object_key, content_length, data, content_md5=None):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    object_key = urllib.parse.quote(object_key.encode('utf-8'))

    headers = {
        'Content-Length': str(content_length),
    }
    if content_md5:
        headers['Content-MD5'] = content_md5
    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='PUT', 
        uri='/{}/{}'.format(bucket, object_key), 
        headers=headers, 
        data=data,
        stream=True
    )


# POST /<bucket>/<object-name>?uploads
def _send_create_multipart(region, host, access_key, access_key_secret, bucket, object_key):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    object_key = urllib.parse.quote(object_key.encode('utf-8'))
    params = {'uploads': ''}

    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='POST',
        uri='/{}/{}'.format(bucket, object_key),
        query_params=params
    )


# PUT /<bucket>/<object-name>?PartNumber=<part_number>&UploadId=<upload_id>
def _send_upload_part(region, host, access_key, access_key_secret,
                      bucket, object_key, content_length, file_like_object, part_number, upload_id, content_md5=None):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    object_key = urllib.parse.quote(object_key.encode('utf-8'))
    params = {'uploadId': upload_id,
              'partNumber': str(part_number)
              }

    headers = {
        'Content-Length': str(content_length),
    }
    if content_md5:
        headers['Content-MD5'] = content_md5
    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='PUT',
        uri='/{}/{}'.format(bucket, object_key),
        query_params=params,
        headers=headers,
        data=file_like_object,
        stream=True
    )


# POST /<bucket>/<object-name>?UploadID=<upload_id>
def _send_complete_multipart(region, host, access_key, access_key_secret, bucket, object_key, upload_id, data):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    object_key = urllib.parse.quote(object_key.encode('utf-8'))
    params = {'uploadId': upload_id}

    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='POST',
        uri='/{}/{}'.format(bucket, object_key), query_params=params, data=data
    )


# DELETE /<bucket>/<object-name>?UploadID=<upload_id>
def _send_abort_multipart(region, host, access_key, access_key_secret, bucket, object_key, upload_id):
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html

    bucket = urllib.parse.quote(bucket.encode('utf-8'))
    object_key = urllib.parse.quote(object_key.encode('utf-8'))
    params = {'uploadId': upload_id}

    return _send_sig4_request(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        method='DELETE',
        uri='/{}/{}'.format(bucket, object_key),
        query_params=params
    )


# send request with sig4 headers
@log_http_request
def _send_sig4_request(region, host, access_key, access_key_secret,
                       method, uri, query_params=None, headers=None, data=None, stream=False):
    # https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
    assert region
    assert host
    assert access_key
    assert access_key
    assert uri

    # Create a date for headers and the credential string
    t = datetime.utcnow()
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = t.strftime('%Y%m%d')

    #
    # Generate the string to sign
    #

    # generate canonical uri string
    canonical_uri = uri if uri else '/'

    # generate canonical query string
    canonical_query = ''
    if query_params:
        sorted_lowered = OrderedDict()
        for name in sorted(query_params.keys()):
            sorted_lowered[name] = query_params[name].encode('utf-8')
        canonical_query = urllib.parse.urlencode(sorted_lowered).replace('+', '%20')

    # generate canonical header string
    headers = {} if headers is None else headers
    headers['host'] = host
    headers['x-amz-date'] = amz_date
    headers['x-amz-content-sha256'] = 'UNSIGNED-PAYLOAD' if data else hashlib.sha256(''.encode('utf-8')).hexdigest()
    sorted_lowered = []
    for header in sorted(headers.keys()):
        sorted_lowered.append((header.lower().strip(), headers[header]))
    canonical_headers = '\n'.join(['{}:{}'.format(k, v) for k, v in sorted_lowered]) + '\n'

    # generate signed headers string
    signed_headers = ';'.join([k for k, v in sorted_lowered])

    # generate body signature
    payload_hash = 'UNSIGNED-PAYLOAD' if data else hashlib.sha256(''.encode('utf-8')).hexdigest()

    # assemble parts into canonical request string
    canonical_request = '\n'.join([
        method.upper(),
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers,
        payload_hash
    ])

    #
    # Generate the string to sign
    #

    # calculate credential scope
    credential_scope = '/'.join([
        date_stamp,
        region,
        's3',
        'aws4_request'
    ])

    # calculate string to sign
    string_to_sign = '\n'.join([
        'AWS4-HMAC-SHA256',  # hashing algorithm
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    ])

    #
    # Generate the signing key
    #

    # https://docs.aws.amazon.com/general/latest/gr/sigv4-date-handling.html
    date_key = hmac.new(
        ('AWS4' + access_key_secret).encode('utf-8'),
        date_stamp.encode('utf-8'),
        hashlib.sha256
    ).digest()

    date_region_key = hmac.new(
        date_key,
        region.encode('utf-8'),
        hashlib.sha256
    ).digest()

    date_region_service_key = hmac.new(
        date_region_key,
        b's3',
        hashlib.sha256
    ).digest()

    signing_key = hmac.new(
        date_region_service_key,
        b'aws4_request',
        hashlib.sha256
    ).digest()

    #
    # Generate signature
    #

    # calculate signature
    signature = hmac.new(
        signing_key,
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    #
    # send request
    #

    # add signing info to request
    headers['Authorization'] = '{} Credential={}/{}, SignedHeaders={}, Signature={}'.format(
        'AWS4-HMAC-SHA256',  # hash algorithm
        access_key,
        credential_scope,
        signed_headers,
        signature
    )

    response = request_session.request(
        method,  # method
        'https://{}{}'.format(host, uri),
        params=query_params,
        headers=headers,
        data=data,
        stream=stream,
        timeout=(_config['request.connection.timeout'], _config['requests.read.timeout'])
    )
    return response


#
# config
#

def update_config(config):
    _config.update(config)

    if config.get('log.enable'):
        assert config.get('log.file.path')
        assert os.path.exists(os.path.dirname(config['log.file.path']))

        # setup logging
        handler = logging.handlers.RotatingFileHandler(
            _config['log.file.path'],
            maxBytes=1024 * 1024 * 1024,  # 1 gb
            backupCount=2
        )
        handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        # startup message
        logging.getLogger(__name__).info('Amazon S3 logging started')


_config = {
    'log.file.path': 's3.log',
    'log.enable': False,
    'request.connection.timeout': 10,
    'requests.read.timeout': 30
}


request_session = requests.Session()
