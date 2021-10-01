import errno
import json
import os
import time
import random


# store object to proxygen datastore
def put(key, object, type):

    file_path = os.path.join(_config['path'], type, key)
    temp_path = file_path + '~' + str(random.randint(1, 1000000))

    # ensure type folder exists
    try:
        os.makedirs(os.path.join(_config['path'], type))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # write object to temp_path
    with open(temp_path, 'w') as data_file:
        json.dump(object, data_file, indent=4, sort_keys=True)

    # replace with temp file - last write wins
    try:
        os.remove(file_path)
    except OSError as e:
        # allow file does not exists, which is expected
        if e.errno != errno.ENOENT:
            # handle unexpected
            raise

    try:
        os.rename(temp_path, file_path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            # handle contention since file was just removed before rename
            return False

        # handle unexpected
        raise

    return True


# get object from proxygen datastore
def get(key, type, ttl_seconds=0):

    file_path = os.path.join(_config['path'], type, key)

    if not os.path.exists(file_path):
        # handle no key for type
        return None

    try:
        # get modified time
        mod_time = os.path.getmtime(file_path)

        # return if ttl expired
        if ttl_seconds != 0 and (mod_time + ttl_seconds < time.time()):
            # cleanup
            delete(key, type)
            # handle expired
            return None

        # load data
        with open(file_path, 'r') as data_file:
            data = json.load(data_file)
        data['datastoreModTime'] = mod_time

    except (IOError, OSError) as e:
        if e.errno == errno.ENOENT:
            # handle file does not exist, which means it's in the middle of update
            raise DatastoreException()

        # handle unexpected
        raise

    return data


# delete object in proxygen datastore
def delete(key, type):

    path = os.path.join(_config['path'], type, key)

    try:
        os.remove(path)
    except OSError as e:
        # ok if path doesn't exist - everything else is unexpected
        if e.errno == errno.ENOENT:
            # handle file not found, which is expected
            return

        # handle unexpected
        raise


class DatastoreException(Exception):
    pass


#
# config
#

_config = {
    'path': None
}


def update_config(properties):
    _config.update(properties)
