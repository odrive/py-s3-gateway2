import xmltodict
from datetime import datetime
import requests_toolbelt
import s3_gateway.util.s3
import s3_gateway.util.metadata_id


def create_file(region, host, access_key, access_key_secret, bucket,
                key_prefix, file_name, size, modified, data):
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
    response_header = _upload_file(region, host, access_key, access_key_secret, bucket, object_key, size, data)
    if response_header is None:
        # Not allowed.
        return None
    if response_header.get('Content-Length'):
        size = int(response_header['Content-Length'])
    # Return s3_obj as metadata.
    return {
        'gateway.metadata.id': s3_gateway.util.metadata_id.metadata_id(object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': file_name,
        'gateway.metadata.modified': modified,
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
    result = s3_gateway.util.s3.create(
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
        'gateway.metadata.id': s3_gateway.util.metadata_id.metadata_id(object_key),
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
    result = s3_gateway.util.s3.delete(
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
        descendants_xml = s3_gateway.util.s3.list_objects(
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
            response = s3_gateway.util.s3.delete_multi(
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

    result = s3_gateway.util.s3.get_object(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )

    # Generate metadata.
    last_modified = int(
        (datetime.strptime(result.get('Last-Modified'), '%Y-%m-%dT%H:%M:%S.%fZ')
         - datetime(1970, 1, 1)).total_seconds() * 1000
    )
    return {
        'gateway.metadata.id': s3_gateway.util.metadata_id.metadata_id(object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': s3_gateway.util.metadata_id.object_name(object_key),
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
    return s3_gateway.util.s3.get_data_iterator(
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
    result = s3_gateway.util.s3.list_objects(
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
            'gateway.metadata.id': s3_gateway.util.metadata_id.metadata_id(file_obj['Key']),
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
            'gateway.metadata.id': s3_gateway.util.metadata_id.metadata_id(prefix['Prefix']),
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
        new_object_key = new_prefix + s3_gateway.util.metadata_id.object_name(object_key)
    else:
        # Move to root.
        new_object_key = s3_gateway.util.metadata_id.object_name(object_key)
    copy_object_response = s3_gateway.util.s3.copy_object(
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
    s3_gateway.util.s3.delete(
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
        'gateway.metadata.id': s3_gateway.util.metadata_id.metadata_id(new_object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': s3_gateway.util.metadata_id.object_name(new_object_key),
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

    source_object = s3_gateway.util.s3.get_object(
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
    new_object_key = object_key.rstrip(s3_gateway.util.metadata_id.object_name(object_key))
    new_object_key = new_object_key + new_name
    copy_object_response = s3_gateway.util.s3.copy_object(
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
    assert s3_gateway.util.s3.delete(
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
        'gateway.metadata.id': s3_gateway.util.metadata_id.metadata_id(new_object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': new_name,
        'gateway.metadata.modified': modified,
        'gateway.metadata.parent.id': None,

        'gateway.metadata.file.size': int(source_object['Content-Length']),
        'gateway.metadata.file.hash': copy_object_response['CopyObjectResult']['ETag'],
    }


def update_file(region, host, access_key, access_key_secret, bucket, object_key, data, size, modified):
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
    response_header = _upload_file(
        region, host, access_key, access_key_secret, bucket, object_key, size, data
    )
    if response_header is None:
        # Not allowed.
        return None

    # Success.
    return {
        'gateway.metadata.id': s3_gateway.util.metadata_id.metadata_id(object_key),
        'gateway.metadata.type': 'file',
        'gateway.metadata.name': s3_gateway.util.metadata_id.object_name(object_key),
        'gateway.metadata.modified': modified,
        'gateway.metadata.parent.id': None,

        'gateway.metadata.file.hash': response_header['ETag'],
        'gateway.metadata.file.size': int(response_header['Content-Length']),
    }


def _upload_file(region, host, access_key, access_key_secret, bucket, object_key, size, data):
    assert region
    assert host
    assert access_key
    assert access_key_secret
    assert bucket
    assert object_key
    assert size >= 0

    # upload file smaller than 2GB
    if size < 1024 * 1024 * 1024 * 2:
        return s3_gateway.util.s3.create(
            region=region,
            host=host,
            access_key=access_key,
            access_key_secret=access_key_secret,
            bucket=bucket,
            object_key=object_key,
            content_length=size,
            data=requests_toolbelt.StreamingIterator(size, data)
            # file_like_object=StreamingIterator(size, file_like_object)
        )

    # upload file bigger than 2GB with multipart upload

    # initialize multipart upload ID
    response = s3_gateway.util.s3.create_multipart_upload(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key
    )
    response = xmltodict.parse(response)
    upload_id = response['InitiateMultipartUploadResult'].get('UploadId')

    # read the request input and serially upload in parts
    file_size = size
    part_size = 1024 * 1024 * 1024 * 1  # 1GB chunk size
    current_part_number = 1
    total_bytes_sent = 0
    uploaded_parts = []
    while total_bytes_sent < file_size:

        # calculate current stream position
        bytes_left = file_size - total_bytes_sent
        if bytes_left < part_size:
            part_size = bytes_left

        # make a generator for reading the next part
        part = _generate_part(data, part_size)

        # make an iterator for streaming the next part
        # (NOTE: StreamingIterator does not stop at size, it reads to end of generator.)
        input_stream = requests_toolbelt.StreamingIterator(part_size, part)

        # upload part to multipart upload session
        part_result = s3_gateway.util.s3.upload_part(
            region=region,
            host=host,
            access_key=access_key,
            access_key_secret=access_key_secret,
            bucket=bucket,
            object_key=object_key,
            content_length=part_size,
            file_like_object=input_stream,
            part_number=current_part_number,
            upload_id=upload_id
        )
        uploaded_parts.append(part_result['Etag'])
        total_bytes_sent += part_size
        current_part_number += 1

    # complete the multipart upload and get the result
    response = s3_gateway.util.s3.complete_multipart_upload(
        region=region,
        host=host,
        access_key=access_key,
        access_key_secret=access_key_secret,
        bucket=bucket,
        object_key=object_key,
        upload_id=upload_id,
        uploaded_parts=uploaded_parts
    )
    return xmltodict.parse(response).get("CompleteMultipartUploadResult")


def _generate_part(upload, limit):
    uploaded_bytes = 0
    chunk_size = 32768
    while True:
        if limit <= uploaded_bytes:
            break
        out = upload.read(chunk_size)
        if not out:
            break
        uploaded_bytes += chunk_size
        yield out
