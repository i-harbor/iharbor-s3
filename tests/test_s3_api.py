"""
pip3 install boto3
"""
import os
import hashlib
import random
import io
from datetime import datetime
from string import printable

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


BUCKET_NAME = 'test-harbor-s3'
FILENAME = './s3.data'
AWS_ACCESS_KEY_ID = 'xxx'
AWS_SECRET_ACCESS_KEY = 'xxx'

config = Config(s3={'addressing_style': 'virtual'})
S3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  endpoint_url='http://s3.obs.cstcloud.cn',
                  use_ssl=False,
                  verify=False,
                  config=config)


def random_string(length: int = 10):
    return random.choices(printable, k=length)


def random_bytes_io(mb_num: int):
    bio = io.BytesIO()
    for i in range(1024):           # MB
        s = ''.join(random_string(mb_num))
        b = s.encode() * 1024         # KB
        n = bio.write(b)

    bio.seek(0)
    return bio


def generate_file(filename, mb_num):
    data = random_bytes_io(mb_num)
    with open(filename, 'wb+') as f:
        s = mb_num * 1024 ** 2 + 1
        d = data.read(s)
        f.write(d)


def remove_file(filename):
    os.remove(filename)


def chunks(f, chunk_size=2*2**20):
    """
    Read the file and yield chunks of ``chunk_size`` bytes (defaults to
    ``File.DEFAULT_CHUNK_SIZE``).
    """
    try:
        f.seek(0)
    except AttributeError:
        pass

    while True:
        data = f.read(chunk_size)
        if not data:
            break
        yield data


def file_upload_part(s3, bucket_name, key, upload_id, filename, part_size):
    parts = []
    part_num = 1
    with open(filename, 'rb') as f:
        for body in chunks(f, chunk_size=part_size):
            if not body:
                break

            r = s3.upload_part(
                Body=body,
                Bucket=bucket_name,
                ContentLength=len(body),
                # ContentMD5=get_bytes_md5(body),
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                # SSECustomerAlgorithm='string',
                # SSECustomerKey='string',
                # RequestPayer='requester'
            )
            etag = r['ETag']
            parts.append({'PartNumber': part_num, 'ETag': etag})
            part_num += 1

        print(parts)
        return parts


def multipart_upload_object_not_complete(s3, bucket: str, object_key: str, filename: str, part_size: int = 10*1024**2,
                            expires=datetime(2020, 7, 14)):
    """
    :return:
        upload_id, parts
    """
    r = s3.create_multipart_upload(
        ACL='private',
        Bucket=bucket,
        # Expires=expires,
        Key=object_key
    )
    upload_id = r['UploadId']
    print(f'@@@ [OK], create_multipart_upload, UploadId={upload_id}')

    parts = file_upload_part(s3=s3, bucket_name=bucket, key=object_key, upload_id=upload_id,
                             filename=filename, part_size=part_size)

    return upload_id, parts


def complete_multipart_upload(s3, bucket, object_key, upload_id, parts):
    r = s3.complete_multipart_upload(
        Bucket=bucket,
        Key=object_key,
        MultipartUpload={
            'Parts': parts
        },
        UploadId=upload_id,
        # RequestPayer='requester'
    )
    return r


def multipart_upload_object(s3, bucket: str, object_key: str, filename: str, part_size: int = 10*1024**2,
                            expires=datetime(2020, 7, 14)):
    upload_id, parts = multipart_upload_object_not_complete(s3=s3, bucket=bucket, object_key=object_key,filename=filename,
                                                            part_size=part_size, expires=expires)

    r = complete_multipart_upload(s3, bucket, object_key, upload_id, parts)
    return upload_id, r


def abort_multipart_upload(s3, bucket: str, object_key: str, upload_id):
    r = s3.abort_multipart_upload(
        Bucket=bucket,
        Key=object_key,
        UploadId=upload_id,
        # RequestPayer='requester'
    )
    return r


def calculate_multipart_s3_etag(file_path, chunk_size=8 * 1024 * 1024):
    md5s = []

    with open(file_path, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))

    digests = b''.join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))


def calculate_md5(filename):
    with open(filename, 'rb') as f:
        md5obj = hashlib.md5()
        for data in chunks(f):
            md5obj.update(data)

        _hash = md5obj.hexdigest()

    return _hash


def assert_error_code(err, codes: list):
    error = err.response.get('Error', {})
    c = error.get('Code', '').lower()
    if c in codes:
        return True

    return False


def test_create_bucket(s3, bucket_name):
    try:
        r = s3.create_bucket(
            ACL='public-read',  # 'private',
            Bucket=bucket_name
        )
    except ClientError as e:
        if not assert_error_code(e, ['BucketAlreadyOwnedByYou']):
            raise e
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 200:
            print(r)
            raise Exception
        else:
            print('@@@ [OK] test_create_bucket')


def test_head_bucket(s3, bucket_name):
    try:
        r = s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        raise e
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 200:
            print(r)
            raise Exception
        else:
            print('@@@ [OK] test_head_bucket')


def test_list_buckets(s3):
    try:
        r = s3.list_buckets()
    except ClientError as e:
        raise e
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 200:
            print(r)
            raise Exception
        else:
            print('@@@ [OK], test_list_buckets')
            print('Existing buckets:')
            for b in r['Buckets']:
                print(f'{b["Name"]}')


def test_put_object(s3, bucket_name, key, filename):
    md5 = calculate_md5(filename)
    body = open(filename, 'rb')
    try:
        r = s3.put_object(
            ACL='private',
            Body=body,
            Bucket=bucket_name,
            # ContentLength=0,#get_size(body),
            # ContentMD5='string',
            Key=key
        )
    except ClientError as e:
        raise e
    else:
        etag = r.get('ETag', '')
        if etag.strip('"') != md5:
            print(f'@@@ [Failed], test_put_object, etag({etag}) != md5({md5})')
            raise Exception
        else:
            print(f'@@@ [OK], test_put_object, etag({etag}) == md5({md5})')


def test_get_object(s3, bucket_name, key):
    try:
        r = s3.get_object(
            Bucket=bucket_name,
            Key=key,
            # Range='bytes=0-0',
            # ResponseContentDisposition='test',
            # ResponseContentEncoding='string',
            # ResponseContentLanguage='string',
            # ResponseContentType='application/xml',
            # PartNumber=2
        )
    except ClientError as e:
        print(f'@@@ [Failed], test_get_object, {str(e)}')
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 200:
            print(f'@@@ [Failed], test_get_object, status_code != 200')
        else:
            print(f'@@@ [OK], test_get_object')


def test_head_object(s3, bucket_name, key):
    try:
        r = s3.head_object(
            Bucket=bucket_name,
            # IfMatch='ab210fa69a3a634a2569b87e93c581ca',
            # IfModifiedSince=datetime(2020, 7, 24, 2, 7),
            # IfNoneMatch='ab210fa69a3a634a2569b87e93c581cb',
            # IfUnmodifiedSince=datetime(2020, 6, 23, 8, 16),
            Key=key,
            # Range='bytes=0-200',
            # PartNumber=1
        )
    except ClientError as e:
        print(f'@@@ [Failed], test_head_object, {str(e)}')
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 200:
            print(f'@@@ [Failed], test_head_object, status_code != 200')
        else:
            print(f'@@@ [OK], test_head_object')


def test_download_file(s3, bucket_name, key, filename='download.data', etag=''):
    try:
        r = s3.download_file(Bucket=bucket_name, Key=key, Filename=filename)
    except ClientError as e:
        raise e
    else:
        if not etag:
            print(f'@@@ [OK], test_download_file')

        md5 = calculate_md5(filename)
        if etag != md5:
            print(f'@@@ [Failed], test_download_file, etag({etag}) != md5({md5})')
            raise Exception
        else:
            print(f'@@@ [OK], test_download_file, etag({etag}) == md5({md5})')


def test_list_objects_v2(s3, bucket_name, prefix='', max_keys=10):
    try:
        r = s3.list_objects_v2(
            Bucket=bucket_name,
            Delimiter='/',
            EncodingType='url',
            MaxKeys=max_keys,
            Prefix=prefix,
            # ContinuationToken='cD0y',
            FetchOwner=True,
            # StartAfter='p1713352617.jpg'
        )
    except ClientError as e:
        print(f'@@@ [Failed], test_list_objects_v2, {str(e)}')
    else:
        ok = True
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 200:
            ok = False
            print(f'@@@ [Failed], test_list_objects_v2, status_code != 200')
        if r.get('Name') != bucket_name:
            ok = False
            print(f'@@@ [Failed], test_list_objects_v2, response.Name({r.get("Name")}) != bucket({bucket_name})')
        if r.get('Prefix') != prefix:
            ok = False
            print(f'@@@ [Failed], test_list_objects_v2, response.Prefix({r.get("Prefix")}) != Prefix({prefix})')
        if r.get('Delimiter') != '/':
            ok = False
            print(f'@@@ [Failed], test_list_objects_v2, response.Delimiter({r.get("Delimiter")}) != Delimiter(/)')
        if r.get('MaxKeys') != max_keys:
            ok = False
            print(f'@@@ [Failed], test_list_objects_v2, response.MaxKeys({r.get("MaxKeys")}) != maxKeys({max_keys})')
        if ok:
            print(f'@@@ [OK], test_list_objects_v2')


def test_delete_object(s3, bucket_name, key):
    try:
        r = s3.delete_object(
            Bucket=bucket_name,
            Key=key
        )
    except ClientError as e:
        print(f'@@@ [Failed], test_delete_object, {str(e)}')
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 204:
            print(f'@@@ [Failed], test_delete_object, status_code != 204')
        else:
            print(f'@@@ [OK], test_delete_object')


def test_delete_objects(s3, bucket_name, keys: list):
    keys.append('a/b/c/d/e')        # 增加一个不存在的key
    objects = [{"Key": k} for k in keys]
    try:
        r = s3.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': objects,
                'Quiet': False
            },
        )
    except ClientError as e:
        print(f'@@@ [Failed], test_delete_objects, {str(e)}')
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 200:
            print(f'@@@ [Failed], test_delete_objects, status_code != 200')
        else:
            ok = True
            deleted = r.get('Deleted', [])
            errors = r.get('Errors', [])
            if (len(deleted) + len(errors)) != len(keys):
                ok = False
                print(f'@@@ [Failed], test_delete_objects, response.Deleted + Errors != 请求删除的对象keys的数量')

            for o in deleted:
                if not o.get('Key') in keys:
                    ok = False
                    print(f'@@@ [Failed], test_delete_objects, response.Deleted包含有无关的对象key')
                    break

            for o in errors:
                if not o.get('Key') in keys:
                    ok = False
                    print(f'@@@ [Failed], test_delete_objects, response.Errors包含有无关的对象key')
                    break
            if ok:
                print(f'@@@ [OK], test_delete_objects')


def test_abort_multipart_upload(s3, bucket_name, key, filename, part_size):
    upload_id, parts = multipart_upload_object_not_complete(s3=s3, bucket=bucket_name, object_key=key,
                                                            filename=filename, part_size=part_size)

    prefix = key.rsplit('/', maxsplit=1)[0]
    try:
        r = s3.list_multipart_uploads(
            Bucket=bucket_name,
            # Delimiter='/',
            EncodingType='url',
            # KeyMarker='a/b/1.txt',
            MaxUploads=66,
            Prefix=prefix,
            # UploadIdMarker='',
        )
    except ClientError as e:
        print(f'@@@ [Failed], test_list_multipart_upload, {str(e)}')
    else:
        uploads = r.get('Uploads', [])
        is_get = ''
        for up in uploads:
            if up.get('Key') == key:
                up_id = up.get('UploadId', '')
                if upload_id == up_id:
                    is_get = up_id
                    break
        if is_get:
            print(f'@@@ [OK], test_list_multipart_upload, get UploadId={is_get}')
        else:
            print(f'@@@ [OK], test_list_multipart_upload, not found UploadId')

    try:
        r = abort_multipart_upload(s3=s3, bucket=bucket_name, object_key=key, upload_id=upload_id)
    except ClientError as e:
        print(f'@@@ [Failed], abort_multipart_upload, {str(e)}; uploadId={upload_id}; parts={parts}')
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 204:
            print(f'@@@ [Failed], abort_multipart_upload, status_code != 204')
        else:
            print(f'@@@ [OK], abort_multipart_upload')


def test_multipart_upload(s3, bucket_name, key, filename, part_size):
    etag = calculate_multipart_s3_etag(filename, chunk_size=part_size)
    upload_id, r = multipart_upload_object(s3=s3, bucket=bucket_name, object_key=key, filename=filename,
                                           part_size=part_size)
    r_etg = r['ETag']
    if etag == r_etg:
        print(f'@@@ [OK], test_multipart_upload; ETag({etag}) == return ETag({r_etg})')
    else:
        print(f'@@@ [Failed], test_multipart_upload; ETag({etag}) != return ETag({r_etg})')


def test_delete_bucket(s3, bucket_name):
    try:
        r = s3.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        if assert_error_code(e, ['BucketNotEmpty', 'NoSuchBucket']):
            print(f'@@@ [OK], test_delete_bucket, {str(e)}')
        else:
            print(f'@@@ [Failed], test_delete_bucket, {str(e)}')
    else:
        status_code = r.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        if status_code != 204:
            print(f'@@@ [Failed], test_delete_bucket, status_code != 204')
        else:
            print(f'@@@ [OK], test_delete_bucket')


if __name__ == "__main__":
    mb_num = 25
    generate_file(FILENAME, mb_num)         # 生成一个上传用的文件
    file_md5 = calculate_md5(FILENAME)      # 文件MD5

    Prefix = 'a/'
    Key = f'{Prefix}s3_object_{mb_num}MB.data'
    part_size = 5*1024*1024                # 多部分上传5Mb分片

    # test_create_bucket(s3=S3, bucket_name=BUCKET_NAME)
    # test_head_bucket(s3=S3, bucket_name=BUCKET_NAME)
    # test_list_buckets(s3=S3)
    # test_put_object(s3=S3, bucket_name=BUCKET_NAME, key=Key, filename=FILENAME)
    # test_head_object(s3=S3, bucket_name=BUCKET_NAME, key=Key)
    # test_get_object(s3=S3, bucket_name=BUCKET_NAME, key=Key)
    # test_download_file(s3=S3, bucket_name=BUCKET_NAME, key=Key, etag=file_md5)
    # test_list_objects_v2(s3=S3, bucket_name=BUCKET_NAME, prefix=Prefix)
    # test_delete_object(s3=S3, bucket_name=BUCKET_NAME, key=Key)

    # multipart_key = f'{Prefix}s3_multipart_object_{mb_num}MB.data'
    # test_abort_multipart_upload(s3=S3, bucket_name=BUCKET_NAME, key=multipart_key, filename=FILENAME,
    #                             part_size=part_size)
    # test_multipart_upload(s3=S3, bucket_name=BUCKET_NAME, key=multipart_key, filename=FILENAME,
    #                       part_size=part_size)
    # test_delete_objects(s3=S3, bucket_name=BUCKET_NAME, keys=[multipart_key])
    # test_delete_bucket(s3=S3, bucket_name=BUCKET_NAME)

    remove_file(FILENAME)
