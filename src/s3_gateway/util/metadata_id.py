import base64


def object_key(gateway_metadata_id):
    return base64.urlsafe_b64decode(gateway_metadata_id).decode('utf-8')


def metadata_id(s3_object_key):
    return base64.urlsafe_b64encode(s3_object_key.encode('utf-8')).decode('ascii')


def object_name(s3_object_key):
    assert s3_object_key[-1] != '/'
    return s3_object_key.split('/')[-1]
