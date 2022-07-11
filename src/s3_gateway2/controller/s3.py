import base64
import hashlib
import xmltodict
from datetime import datetime
import requests_toolbelt
import s3_gateway2.util.s3
import s3_gateway2.util.metadata_id


def create_file(region, host, access_key, access_key_secret, bucket,
                key_prefix, file_name, size, modified, data, sha256=None):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert file_name

    # Calculate new S3 object key.
    if key_prefix:
        object_key = f"{key_prefix}{file_name}"
    else:
        object_key = file_name

    # Upload file.
    response_header = s3_gateway2.util.s3.create(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key,
        content_length=size,
        data=requests_toolbelt.StreamingIterator(
            size,
            _file_stream_generator(
                upload=data,
                limit=size,
                expected_sha256=sha256
            )
        ),
    )

    if response_header is None:
        # Not allowed.
        return None
    if response_header.get('Content-Length') is not None:
        size = int(response_header['Content-Length'])
    # Return s3_obj as metadata.
    return {
        'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': file_name,
        'gateway.metadata.modified': None,  # starts out unset
        'gateway.metadata.parent.id': None,

        'gateway.metadata.file.size': size,
        'gateway.metadata.file.hash': response_header['ETag'],
    }


def create_folder(region, host, access_key, access_key_secret, bucket, key_prefix, folder_name):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert folder_name

    # Create empty file as folder marker.
    if key_prefix:
        object_key = f"{key_prefix}{folder_name}/"
    else:
        object_key = f"{folder_name}/"
    result = s3_gateway2.util.s3.create(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key,
        content_length=0,
        data=''
    )
    if result is None:
        # Not allowed.
        return None

    # Success.
    return {
        'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(object_key),
        'gateway.metadata.type': 'folder',
        'gateway.metadata.name': folder_name,
        'gateway.metadata.modified': None,
        'gateway.metadata.parent.id': None,
    }


def delete_file(region, host, access_key, access_key_secret, bucket, object_key):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key

    #
    # Validate.
    #

    #
    # Execute.
    #

    # Delete file.
    result = s3_gateway2.util.s3.delete(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )
    if result is None:
        # Not allowed.
        return False

    # Success.
    return True


def delete_folder(region, host, access_key, access_key_secret, bucket, object_prefix):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_prefix

    #
    # Validate.
    #

    #
    # Execute.
    #

    # Delete folder.
    continuation_token = None
    while True:

        # get children keys:
        child_keys = []

        # list all objects with prefix, no delimiter to get all descendants
        descendants_xml = s3_gateway2.util.s3.list_objects(
            region=region,
            host=host,
            access_key=access_key,
            access_key_secret=access_key_secret,
            bucket=bucket,
            prefix=object_prefix,
            continuation_token=continuation_token,
            delimiter=None
        )
        assert descendants_xml

        # convert xml to ordered dict for processing
        descendants_dict = xmltodict.parse(descendants_xml)

        # extract files
        if descendants_dict['ListBucketResult'].get('Contents'):
            if isinstance(descendants_dict['ListBucketResult']['Contents'], list):
                child_keys.extend([c['Key'] for c in descendants_dict['ListBucketResult']['Contents']])
            else:
                child_keys.append(descendants_dict['ListBucketResult']['Contents']['Key'])

        # bulk delete
        if child_keys:
            response = s3_gateway2.util.s3.delete_multi(
                region=region,
                host=host,
                access_key=access_key,
                access_key_secret=access_key_secret,
                bucket=bucket,
                object_keys=child_keys
            )
            if response is None:
                return False

        # done if no more
        if descendants_dict['ListBucketResult'].get('IsTruncated') != 'true':
            break

        # delete next page
        continuation_token = descendants_dict['ListBucketResult']['NextContinuationToken']

    # success
    return True


def get_file_metadata(region, host, access_key, access_key_secret, bucket, object_key):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key

    result = s3_gateway2.util.s3.get_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    if result is None:
        # Not found or not allowed.
        return None

    # Generate metadata.
    last_modified = int(
        (datetime.strptime(result.get('Last-Modified'), '%Y-%m-%dT%H:%M:%S.%fZ')
         - datetime(1970, 1, 1)).total_seconds() * 1000
    )
    return {
        'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': s3_gateway2.util.metadata_id.object_name(object_key),
        'gateway.metadata.modified': last_modified,
        'gateway.metadata.parent.id': None,

        'gateway.metadata.file.size': int(result['Content-Length']),
        'gateway.metadata.file.hash': result['ETag'],
    }


def iter_file(region, host, access_key, access_key_secret, bucket, object_key):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key

    # stream data
    return s3_gateway2.util.s3.get_data_iterator(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )


def list_content(region, host, access_key, access_key_secret, bucket, prefix, continuation_token=None):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    # assert prefix

    # list metadata for objects with delimiter and source ID as prefix
    result = s3_gateway2.util.s3.list_objects(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        prefix=prefix,
        continuation_token=continuation_token,
        delimiter='/'
    )
    if result is None:
        # handle not allowed
        return None, None

    # convert XML to ordered dict for easier processing
    result = xmltodict.parse(result)
    if result.get('ListBucketResult') is None:
        # handle not allowed
        return None, None

    # extract file metadata
    file_list = []
    if result['ListBucketResult'].get('Contents'):
        if isinstance(result['ListBucketResult']['Contents'], list):
            # add all content to file list
            file_list.extend(result['ListBucketResult']['Contents'])
        else:
            # add single content to file list
            file_list.append(result['ListBucketResult']['Contents'])

    # extract sub folder metadata
    prefix_list = []
    if result['ListBucketResult'].get('CommonPrefixes'):
        if isinstance(result['ListBucketResult']['CommonPrefixes'], list):
            # add list of prefixes as folders
            prefix_list.extend(result['ListBucketResult']['CommonPrefixes'])
        else:
            # add one prefix as folder
            prefix_list.append(result['ListBucketResult']['CommonPrefixes'])

    # format metadata to response:
    content_listing = []

    # convert file metadata to content resource
    for file_obj in file_list:

        # filter out the prefix marker
        if file_obj['Key'] == prefix:
            continue

        # get name from object key
        name = file_obj['Key'].split('/')[-1]  # extract name from key

        # calculate last modified - millis since epoch
        modified = int(
            (datetime.strptime(file_obj['LastModified'], '%Y-%m-%dT%H:%M:%S.%fZ')
             - datetime(1970, 1, 1)).total_seconds() * 1000
        )

        # assemble file content resource
        content_listing.append({
            'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(file_obj['Key']),
            'gateway.metadata.type': 'file',
            'gateway.metadata.name': name,
            'gateway.metadata.modified': modified,
            'gateway.metadata.parent.id': None,

            'gateway.metadata.file.hash': file_obj['ETag'],
            'gateway.metadata.file.size': int(file_obj['Size']),
        })

    # convert folder list to content resource
    for prefix in prefix_list:
        if prefix['Prefix'] == '/':
            # Skip root.
            continue

        name = prefix['Prefix'].rstrip('/').split('/')[-1]  # extract name from prefix
        content_listing.append({
            'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(prefix['Prefix']),
            'gateway.metadata.type': 'folder',
            'gateway.metadata.name': name,
            'gateway.metadata.modified': None,
            'gateway.metadata.parent.id': None,
        })

    # add continuation token to data
    next_continuation_token = None
    if result['ListBucketResult'].get('IsTruncated') == 'true':
        next_continuation_token = result['ListBucketResult']['NextContinuationToken']

    # send data as content listing and page token
    return content_listing, next_continuation_token


def move(region, host, access_key, access_key_secret, bucket, object_key, new_prefix):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key

    #
    # Load.
    #

    source_object = get_file_metadata(region, host, access_key, access_key_secret, bucket, object_key)

    #
    # Validate.
    #

    # Check source.
    if source_object is None:
        # Not found.
        return None
    if object_key[-1] == '/':
        # Not allowed to move folder.
        return None

    # check destination.
    if new_prefix and new_prefix[-1] != '/':
        # Not folder.
        return None

    #
    # execute request
    #

    # Copy file to target folder.
    if new_prefix:
        # Move to folder.
        new_object_key = new_prefix + s3_gateway2.util.metadata_id.object_name(object_key)
    else:
        # Move to root.
        new_object_key = s3_gateway2.util.metadata_id.object_name(object_key)
    copy_object_response = s3_gateway2.util.s3.copy_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        from_bucket=bucket,
        from_object=object_key,
        to_bucket=bucket,
        to_object=new_object_key
    )
    if copy_object_response is None:
        # Not allowed.
        return None

    # Delete original file.
    s3_gateway2.util.s3.delete(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    # Success.
    modified = int(
        (datetime.strptime(copy_object_response['CopyObjectResult']['LastModified'], '%Y-%m-%dT%H:%M:%S.%fZ')
         - datetime(1970, 1, 1)).total_seconds() * 1000
    )
    return {
        'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(new_object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': s3_gateway2.util.metadata_id.object_name(new_object_key),
        'gateway.metadata.modified': modified,
        'gateway.metadata.parent.id': None,

        'gateway.metadata.file.size': source_object['gateway.metadata.file.size'],
        'gateway.metadata.file.hash': copy_object_response['CopyObjectResult']['ETag'],
    }


def rename(region, host, access_key, access_key_secret, bucket, object_key, new_name):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key
    assert new_name

    source_object = s3_gateway2.util.s3.get_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    #
    # Validate.
    #

    if source_object is None:
        # Not fount.
        return None
    if object_key[-1] == '/':
        # Not allowed to rename folder.
        return None

    #
    # Execute.
    #

    # Copy file to new name.
    new_object_key = object_key.rstrip(s3_gateway2.util.metadata_id.object_name(object_key))
    new_object_key = new_object_key + new_name
    copy_object_response = s3_gateway2.util.s3.copy_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        from_bucket=bucket,
        from_object=object_key,
        to_bucket=bucket,
        to_object=new_object_key
    )
    if copy_object_response is None:
        # Not allowed.
        return None

    # Delete original file
    assert s3_gateway2.util.s3.delete(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    # Success
    modified = int(
        (datetime.strptime(copy_object_response['CopyObjectResult']['LastModified'], '%Y-%m-%dT%H:%M:%S.%fZ')
         - datetime(1970, 1, 1)).total_seconds() * 1000
    )
    return {
        'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(new_object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': new_name,
        'gateway.metadata.modified': modified,
        'gateway.metadata.parent.id': None,

        'gateway.metadata.file.size': int(source_object['Content-Length']),
        'gateway.metadata.file.hash': copy_object_response['CopyObjectResult']['ETag'],
    }


def update_file(region, host, access_key, access_key_secret, bucket, object_key, data, size, modified, sha256=None):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key
    assert data
    assert size is not None
    assert isinstance(size, int)

    # Check file.
    if object_key[-1] == '/':
        # Not a file.
        return None

    # Upload file.
    response_header = s3_gateway2.util.s3.create(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key,
        content_length=size,
        data=requests_toolbelt.StreamingIterator(
            size,
            _file_stream_generator(
                upload=data,
                limit=size,
                expected_sha256=sha256
            )
        ),
    )
    if response_header is None:
        # Not allowed.
        return None

    # Success.
    return {
        'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': s3_gateway2.util.metadata_id.object_name(object_key),
        'gateway.metadata.modified': None,  # starts out unset
        'gateway.metadata.parent.id': None,

        'gateway.metadata.file.hash': response_header['ETag'],
        'gateway.metadata.file.size':
            int(response_header['Content-Length']) if response_header.get('Content-Length') is not None else None,
    }


def create_new_file_upload(region, host, access_key, access_key_secret, bucket, key_prefix, file_name, segments):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert file_name
    assert segments

    # Calculate new S3 object key.
    if key_prefix:
        object_key = f"{key_prefix}{file_name}"
    else:
        object_key = file_name

    # Initialize multipart upload.
    response = s3_gateway2.util.s3.create_multipart_upload(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )
    if response is None:
        # Not allowed.
        return None

    response = xmltodict.parse(response)

    # Insert empty cookies into segment table to satisfy gateway API.
    for segment in segments:
        segment.update({
            'gateway.upload.segment.cookie': {},
        })

    # Return gateway upload
    return {
        'gateway.upload.id': _gateway_upload_id(
            s3_object_key=object_key,
            s3_upload_id=response['InitiateMultipartUploadResult'].get('UploadId')
        ),
        'gateway.upload.segment': segments,
        'gateway.upload.cookie': {
            'upload.key': None
        },
    }


def create_update_file_upload(region, host, access_key, access_key_secret, bucket, object_key, segments):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key
    assert segments

    # Check file.
    if object_key[-1] == '/':
        # Not a file.
        return None

    # Initialize multipart upload.
    response = s3_gateway2.util.s3.create_multipart_upload(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )
    if response is None:
        # Not allowed.
        return None

    response = xmltodict.parse(response)

    # Insert empty cookies into segment table to satisfy gateway API.
    for segment in segments:
        segment.update({
            'gateway.upload.segment.cookie': {},
        })

    # Return gateway upload
    return {
        'gateway.upload.id': _gateway_upload_id(
            s3_object_key=object_key,
            s3_upload_id=response['InitiateMultipartUploadResult'].get('UploadId')
        ),
        'gateway.upload.segment': segments,
        'gateway.upload.cookie': {},
    }


def upload_segment(region, host, access_key, access_key_secret, bucket, segment_number, segment_size,
                   segment_sha256, gateway_upload_id, input_stream):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert segment_number
    assert segment_size
    assert segment_sha256
    assert gateway_upload_id
    assert input_stream

    # Decode gateway upload id
    redirect_upload = _redirect_upload(gateway_upload_id)
    assert redirect_upload

    # upload part to multipart upload session
    part_result = s3_gateway2.util.s3.upload_part(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=redirect_upload['object.key'],
        content_length=segment_size,
        file_like_object=requests_toolbelt.StreamingIterator(
            segment_size,
            _file_stream_generator(
                upload=input_stream,
                limit=segment_size,
                expected_sha256=segment_sha256
            )
        ),
        part_number=segment_number,
        upload_id=redirect_upload['upload.id'],
    )
    if part_result is None:
        # Not allowed
        return None

    # Return updated segment
    return {
        'gateway.upload.id': gateway_upload_id,
        'gateway.upload.segment.number': segment_number,
        'gateway.upload.segment.sha256': segment_sha256,
        'gateway.upload.segment.size': segment_size,
        'gateway.upload.segment.cookie': {
            'etag': part_result['Etag'],
        },
        'gateway.upload.cookie': {}
    }


def complete_upload(region, host, access_key, access_key_secret, bucket, gateway_upload_id, segments, size):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert gateway_upload_id
    assert segments
    assert size is not None

    # Decode gateway upload id
    redirect_upload = _redirect_upload(gateway_upload_id)
    assert redirect_upload

    # Complete the multipart upload and get the result.
    response = s3_gateway2.util.s3.complete_multipart_upload(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=redirect_upload['object.key'],
        upload_id=redirect_upload['upload.id'],
        uploaded_parts=[segment['gateway.upload.segment.cookie']['etag']
            for segment in segments
        ]
    )
    if response:
        response = xmltodict.parse(response).get("CompleteMultipartUploadResult")

    if response is None:
        # Not allowed.
        return None

    # Success.
    return {
        'gateway.metadata.id': s3_gateway2.util.metadata_id.metadata_id(redirect_upload['object.key']),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': s3_gateway2.util.metadata_id.object_name(redirect_upload['object.key']),
        'gateway.metadata.modified': None,  # starts out unset
        'gateway.metadata.parent.id': None,
        'gateway.metadata.file.hash': response['ETag'],
        'gateway.metadata.file.size': size,
    }


def delete_upload(region, host, access_key, access_key_secret, bucket, gateway_upload_id):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert gateway_upload_id

    # Decode gateway upload id
    redirect_upload = _redirect_upload(gateway_upload_id)
    assert redirect_upload

    # Complete the multipart upload and get the result.
    response = s3_gateway2.util.s3.abort_multipart_upload(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=redirect_upload['object.key'],
        upload_id=redirect_upload['upload.id'],
    )
    if response is None:
        # Not allowed.
        return False

    return True


def _file_stream_generator(upload, limit, expected_sha256=None):
    uploaded_bytes = 0
    chunk_size = 32768
    sha256 = hashlib.sha256()
    while True:
        if limit <= uploaded_bytes:
            break
        out = upload.read(chunk_size)
        if not out:
            break
        uploaded_bytes += chunk_size
        if expected_sha256:
            sha256.update(out)
        yield out

    # Transfer completed. Validate against expected sha256.
    if expected_sha256:
        assert expected_sha256 == sha256.hexdigest()


def _redirect_upload(gateway_upload_id):
    # Get redirect info.
    parts = gateway_upload_id.split('::')
    assert len(parts) == 2
    return {
        'object.key': base64.urlsafe_b64decode(parts[0]).decode('utf-8'),
        'upload.id': base64.urlsafe_b64decode(parts[1]).decode('utf-8'),
    }


def _gateway_upload_id(s3_object_key, s3_upload_id):
    encoded_object_key = base64.urlsafe_b64encode(s3_object_key.encode('utf-8')).decode('ascii')
    encoded_s3_upload_id = base64.urlsafe_b64encode(s3_upload_id.encode('utf-8')).decode('ascii')
    return f"{encoded_object_key}::{encoded_s3_upload_id}"
