import os
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists)

from gcloudlogging.logger import create_logger
from time import sleep, time

minio_host = os.environ.get('MINIO_HOST')
minion_access_key = os.environ.get('MINIO_ACCESS_KEY', "minioadmin")
minion_secret_key = os.environ.get('MINIO_SECRET_KEY', "minioadmin")

DEBUG = os.environ.get('DEBUG', False)
log = create_logger()


def _get_storage_client():
    if hasattr(_get_storage_client, 'client'):
        return _get_storage_client.client

    _get_storage_client.client = Minio(minio_host, access_key=minion_access_key,
                                       secret_key=minion_secret_key, secure=False)
    return _get_storage_client.client


def upload_file(file_stream, filename, app_name, metadata, compress=False):
    if DEBUG:
        start = time()
    log.info(f'Uploading file {filename} to {app_name}', extra={'_filename': filename, 'app': app_name})

    client = _get_storage_client()

    stream_length = file_stream.getbuffer().nbytes
    result = ""
    try:
        result = client.put_object(app_name, filename, file_stream, stream_length,
                                   content_type="application/octet-stream", metadata=metadata)
    except ResponseError as e:
        log.error(e, extra={'_filename': filename, 'app': app_name})

    return result


def list_all_files(user_id, folder, app_name):
    client = _get_storage_client()

    prefix = os.path.join(*[str(user_id), folder])
    return client.list_objects(app_name, prefix=prefix)


def list_files(user_id, folder, app_name):
    return [f.object_name.encode('utf-8') for f in list_all_files(user_id, folder, app_name)]


def get_file(filename, appname):
    client = _get_storage_client()
    return client.get_object(appname, filename).text
