import base64


def object_key(content_id):
    return base64.urlsafe_b64decode(content_id).decode('utf-8')


def content_id(object_key):
    return base64.urlsafe_b64encode(object_key.encode('utf-8')).decode('ascii')


def object_name(object_key):
    assert object_key[-1] != '/'
    return object_key.split('/')[-1]
