import base64
import re
from collections import OrderedDict

from django.utils.translation import gettext
from django.http import FileResponse
from django.utils.http import urlquote
from rest_framework.response import Response
from rest_framework import status
from rest_framework.serializers import ValidationError
from rest_framework.exceptions import UnsupportedMediaType
from rest_framework.parsers import FileUploadParser

from buckets.models import Bucket
from utils.storagers import FileUploadToCephHandler, PartUploadToCephHandler
from utils.md5 import EMPTY_BYTES_MD5, EMPTY_HEX_MD5, FileMD5Handler
from utils.oss.pyrados import HarborObject, RadosError
from utils.time import datetime_from_gmt
from buckets.models import BucketFileBase
from . import renders
from .viewsets import CustomGenericViewSet
from .validators import DNSStringValidator, bucket_limit_validator
from .utils import (get_ceph_poolname_rand, BucketFileManagement, create_table_for_model_class,
                    delete_table_for_model_class)
from . import exceptions
from .harbor import HarborManager
from . import serializers
from . import paginations
from .managers import (get_parts_model_class, MultipartUploadManager, ObjectPartManager)
from .negotiation import CusContentNegotiation
from . import parsers
from .models import build_part_rados_key
from .handlers import MULTIPART_UPLOAD_MAX_SIZE
from . import handlers


class BucketViewSet(CustomGenericViewSet):
    http_method_names = ['get', 'post', 'put', 'delete', 'head', 'options']
    renderer_classes = [renders.CusXMLRenderer]
    content_negotiation_class = CusContentNegotiation
    parser_classes = [parsers.S3XMLParser]

    def list(self, request, *args, **kwargs):
        """
        list objects (v1 && v2)
        get object metadata
        ListMultipartUploads
        """
        uploads = request.query_params.get('uploads', None)
        if uploads is not None:
            return self.list_multipart_uploads(request=request, args=args, kwargs=kwargs)

        list_type = request.query_params.get('list-type', '1')
        if list_type == '2':
            return self.list_objects_v2(request=request, args=args, kwargs=kwargs)

        # ListObjectVersions
        if 'versions' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='ListObjectVersions not implemented'))

        return self.list_objects_v1(request=request, args=args, kwargs=kwargs)

    def create(self, request, *args, **kwargs):
        """
        DeleteObjects
        """
        delete = request.query_params.get('delete')
        if delete is not None:
            return self.delete_objects(request)

        return self.exception_response(request, exceptions.S3MethodNotAllowed())

    def update(self, request, *args, **kwargs):
        """
        create bucket

        Headers:
            x-amz-acl:
                The canned ACL to apply to the bucket.
                Valid Values: private | public-read | public-read-write | authenticated-read
        """
        bucket_name = self.get_bucket_name(request)
        if not bucket_name:
            return self.exception_response(request, exceptions.S3InvalidRequest('Invalid request domain name'))

        return self.create_bucket(request, bucket_name)

    def destroy(self, request, *args, **kwargs):
        """
        delete bucket
        """
        return self.delete_bucket(request=request, args=args, kwargs=kwargs)

    def head(self, request, *args, **kwargs):
        """
        head bucket
        """
        return self.head_bucket(request=request, args=args, kwargs=kwargs)

    def delete_bucket(self, request, *args, **kwargs):
        bucket_name = self.get_bucket_name(request)
        if not bucket_name:
            return self.exception_response(request, exceptions.S3InvalidRequest('Invalid request domain name'))

        hm = HarborManager()
        try:
            bucket, qs = hm.get_bucket_objects_dirs_queryset(bucket_name=bucket_name, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        try:
            not_empty = qs.filter(fod=True).exists()        # 有无对象，忽略目录
        except Exception as e:
            return self.exception_response(request, e)
        if not_empty:
            return self.exception_response(request, exceptions.S3BucketNotEmpty())

        if not bucket.delete_and_archive():  # 删除归档
            return self.exception_response(request, exceptions.S3InternalError(gettext('删除存储桶失败')))

        return Response(status=status.HTTP_204_NO_CONTENT)

    def head_bucket(self, request, *args, **kwargs):
        bucket_name = self.get_bucket_name(request)
        if not bucket_name:
            return self.exception_response(request, exceptions.S3InvalidRequest('Invalid request domain name'))

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            return self.exception_response(request, exceptions.S3NoSuchBucket())

        if bucket.is_public_permission():
            return Response(status=status.HTTP_200_OK)

        if not bucket.check_user_own_bucket(user=request.user):
            return self.exception_response(request, exceptions.S3AccessDenied())

        return Response(status=status.HTTP_200_OK)

    @staticmethod
    def validate_create_bucket(request, bucket_name: str):
        """
        创建桶验证

        :return: bucket_name: str
        :raises: ValidationError
        """
        user = request.user

        if not bucket_name:
            raise exceptions.S3BucketNotEmpty()

        if bucket_name.startswith('-') or bucket_name.endswith('-'):
            raise exceptions.S3InvalidBucketName()      # 存储桶bucket名称不能以“-”开头或结尾

        try:
            DNSStringValidator(bucket_name)
        except ValidationError:
            raise exceptions.S3InvalidBucketName()

        bucket_name = bucket_name.lower()

        # 用户存储桶限制数量检测
        try:
            bucket_limit_validator(user=user)
        except ValidationError:
            raise exceptions.S3TooManyBuckets()

        b = Bucket.get_bucket_by_name(bucket_name)
        if b:
            if b.check_user_own_bucket(user):
                raise exceptions.S3BucketAlreadyOwnedByYou()
            raise exceptions.S3BucketAlreadyExists()
        return bucket_name

    def create_bucket(self, request, bucket_name: str):
        """
        创建桶

        :return: Response()
        """
        acl_choices = {'private': Bucket.PRIVATE, 'public-read': Bucket.PUBLIC, 'public-read-write': Bucket.PUBLIC_READWRITE}
        acl = request.headers.get('x-amz-acl', 'private').lower()
        if acl not in acl_choices:
            e = exceptions.S3InvalidRequest('The value of header "x-amz-acl" is invalid and unsupported.')
            return self.exception_response(request, e)

        try:
            bucket_name = self.validate_create_bucket(request, bucket_name)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        user = request.user
        perms = acl_choices[acl]
        pool_name = get_ceph_poolname_rand()
        bucket = Bucket(pool_name=pool_name, user=user, name=bucket_name, access_permission=perms, type=Bucket.TYPE_S3)
        try:
            bucket.save()
        except Exception as e:
            return self.exception_response(request, exceptions.S3InternalError(message=gettext('创建存储桶失败，存储桶元数据错误'), extend_msg=str(e)))

        col_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=col_name)
        model_class = bfm.get_obj_model_class()
        if not create_table_for_model_class(model=model_class):
            bucket.delete()
            delete_table_for_model_class(model=model_class)
            return self.exception_response(request, exceptions.S3InternalError(message=gettext('创建存储桶失败，存储桶object表错误')))

        part_table_name = bucket.get_parts_table_name()
        parts_class = get_parts_model_class(table_name=part_table_name)
        if not create_table_for_model_class(model=parts_class):
            bucket.delete()
            delete_table_for_model_class(model=parts_class)
            delete_table_for_model_class(model=model_class)
            return self.exception_response(request, exceptions.S3InternalError(message=gettext('创建存储桶失败，存储桶parts表错误')))

        return Response(status=status.HTTP_200_OK, headers={'Location': '/' + bucket_name})

    def list_objects_v2(self, request, *args, **kwargs):
        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', '')
        fetch_owner = request.query_params.get('fetch-owner', '').lower()
        bucket_name = self.get_bucket_name(request)

        if not delimiter:    # list所有对象和目录
            return self.list_objects_v2_list_prefix(request=request, prefix=prefix)

        if delimiter != '/':
            return self.exception_response(request, exceptions.S3InvalidArgument(message=gettext('参数“delimiter”必须是“/”')))

        path = prefix.strip('/')
        if prefix and not path:     # prefix invalid, return no match data
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter)

        hm = HarborManager()
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if obj is None:
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter, bucket=bucket)

        paginator = paginations.ListObjectsV2CursorPagination(context={'bucket': bucket})
        max_keys = paginator.get_page_size(request=request)
        ret_data = {
            'IsTruncated': 'false',     # can not use bool
            'Name': bucket_name,
            'Prefix': prefix,
            'EncodingType': 'url',
            'MaxKeys': max_keys,
            'Delimiter': delimiter
        }

        if prefix == '' or prefix.endswith('/'):  # list dir
            if not obj.is_dir():
                return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter, bucket=bucket)

            objs_qs = hm.list_dir_queryset(bucket=bucket, dir_obj=obj)
            paginator.paginate_queryset(objs_qs, request=request)
            objs, _ = paginator.get_objects_and_dirs()

            if fetch_owner == 'true':
                serializer = serializers.ObjectListV2WithOwnerSerializer(objs, many=True, context={'user': request.user})
            else:
                serializer = serializers.ObjectListV2Serializer(objs, many=True)

            data = paginator.get_paginated_data(common_prefixes=True, delimiter=delimiter)
            ret_data.update(data)
            ret_data['Contents'] = serializer.data
            self.set_renderer(request, renders.ListObjectsV2XMLRenderer())
            return Response(data=ret_data, status=status.HTTP_200_OK)

        # list object metadata
        if not obj.is_file():
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter, bucket=bucket)

        if fetch_owner == 'true':
            serializer = serializers.ObjectListV2WithOwnerSerializer(obj, context={'user': request.user})
        else:
            serializer = serializers.ObjectListV2Serializer(obj)

        ret_data['Contents'] = [serializer.data]
        ret_data['KeyCount'] = 1
        self.set_renderer(request, renders.ListObjectsV2XMLRenderer())
        return Response(data=ret_data, status=status.HTTP_200_OK)

    def list_objects_v2_list_prefix(self, request, prefix):
        """
        列举所有对象和目录
        """
        fetch_owner = request.query_params.get('fetch-owner', '').lower()

        bucket_name = self.get_bucket_name(request)
        hm = HarborManager()
        try:
            bucket, objs_qs = hm.get_bucket_objects_dirs_queryset(bucket_name=bucket_name, user=request.user, prefix=prefix)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        paginator = paginations.ListObjectsV2CursorPagination(context={'bucket': bucket})
        objs_dirs = paginator.paginate_queryset(objs_qs, request=request)
        if fetch_owner == 'true':
            serializer = serializers.ObjectListV2WithOwnerSerializer(objs_dirs, many=True, context={'user': request.user})
        else:
            serializer = serializers.ObjectListV2Serializer(objs_dirs, many=True)

        data = paginator.get_paginated_data()
        data['Contents'] = serializer.data
        data['Name'] = bucket_name
        data['Prefix'] = prefix
        data['EncodingType'] = 'url'

        self.set_renderer(request, renders.ListObjectsV2XMLRenderer())
        return Response(data=data, status=status.HTTP_200_OK)

    def list_objects_v2_no_match(self, request, prefix, delimiter, bucket=None):
        if bucket:
            bucket_name = bucket.name
            context = {'bucket': bucket}
        else:
            bucket_name = self.get_bucket_name(request)
            context = {'bucket_name': bucket_name}

        paginator = paginations.ListObjectsV2CursorPagination(context=context)
        max_keys = paginator.get_page_size(request=request)
        ret_data = {
            'IsTruncated': 'false',     # can not use True
            'Name': bucket_name,
            'Prefix': prefix,
            'EncodingType': 'url',
            'MaxKeys': max_keys,
            'KeyCount': 0
        }
        if delimiter:
            ret_data['Delimiter'] = delimiter

        self.set_renderer(request, renders.ListObjectsV2XMLRenderer())
        return Response(data=ret_data, status=status.HTTP_200_OK)

    def list_objects_v1(self, request, *args, **kwargs):
        return handlers.ListObjectsHandler().list_objects(view=self, request=request)

    def list_multipart_uploads(self, request, *args, **kwargs):
        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', None)
        encoding_type = request.query_params.get('encoding-type', None)
        x_amz_expected_bucket_owner = request.headers.get('x-amz-expected-bucket-owner', None)
        bucket_name = self.get_bucket_name(request)

        if delimiter is not None:
            return self.exception_response(request, exceptions.S3InvalidArgument(message=gettext('参数“delimiter”暂时不支持')))

        try:
            bucket = HarborManager().get_public_or_user_bucket(name=bucket_name, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if x_amz_expected_bucket_owner:
            try:
                if bucket.id != int(x_amz_expected_bucket_owner):
                    raise ValueError
            except ValueError:
                return self.exception_response(request, exceptions.S3AccessDenied())

        queryset = MultipartUploadManager().list_multipart_uploads_queryset(bucket_name=bucket_name, prefix=prefix)
        paginator = paginations.ListUploadsKeyPagination(context={'bucket': bucket})

        ret_data = {
            'Bucket': bucket_name,
            'Prefix': prefix
        }
        if encoding_type:
            ret_data['EncodingType'] = encoding_type

        ups = paginator.paginate_queryset(queryset, request=request)
        serializer = serializers.ListMultipartUploadsSerializer(ups, many=True, context={'user': request.user})
        data = paginator.get_paginated_data()
        ret_data.update(data)
        ret_data['Upload'] = serializer.data
        self.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='ListMultipartUploadsResult'))
        return Response(data=ret_data, status=status.HTTP_200_OK)

    def delete_objects(self, request):
        bucket_name = self.get_bucket_name(request)

        body = request.body
        content_b64_md5 = self.request.headers.get('Content-MD5', '')
        md5_hl = FileMD5Handler()
        md5_hl.update(offset=0, data=body)
        bytes_md5 = md5_hl.digest()
        base64_md5 = base64.b64encode(bytes_md5).decode('ascii')
        if content_b64_md5 != base64_md5:
            return self.exception_response(request, exceptions.S3BadDigest())

        try:
            data = request.data
        except Exception as e:
            return self.exception_response(request, exceptions.S3MalformedXML())

        root = data.get('Delete')
        if not root:
            return self.exception_response(request, exceptions.S3MalformedXML())

        keys = root.get('Object')
        if not keys:
            return self.exception_response(request, exceptions.S3MalformedXML())

        # XML解析器行为有关，只有一个item时不是list
        if not isinstance(keys, list):
            keys = [keys]

        if len(keys) > 1000:
            return self.exception_response(request, exceptions.S3MalformedXML(
                message='You have attempted to delete more objects than allowed 1000'))

        deleted_objs, err_objs = HarborManager().delete_objects(bucket_name=bucket_name, obj_keys=keys, user=request.user)

        quiet = root.get('Quiet', 'false').lower()
        if quiet == 'true':     # 安静模式不包含 删除成功对象信息
            data = {'Error': err_objs}
        else:
            data = {'Error': err_objs, 'Deleted': deleted_objs}

        self.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='DeleteResult'))
        return Response(data=data, status=status.HTTP_200_OK)


class ObjViewSet(CustomGenericViewSet):
    http_method_names = ['get', 'post', 'put', 'delete', 'head', 'options']
    renderer_classes = [renders.CusXMLRenderer]
    content_negotiation_class = CusContentNegotiation
    parser_classes = [parsers.S3XMLParser]

    def list(self, request, *args, **kwargs):
        """
        get object
        """
        # GetObjectAcl
        if 'acl' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectAcl not implemented'))

        # GetObjectLegalHold
        if 'legal-hold' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectLegalHold not implemented'))

        # GetObjectLockConfiguration
        if 'object-lock' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectLockConfiguration not implemented'))

        # GetObjectRetention
        if 'retention' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectRetention not implemented'))

        # GetObjectTagging
        if 'tagging' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectTagging not implemented'))

        # GetObjectTorrent
        if 'torrent' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectTorrent not implemented'))

        # ListParts
        if 'uploadId' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='ListParts not implemented'))

        return self.s3_get_object(request=request, args=args, kwargs=kwargs)

    def create(self, request, *args, **kwargs):
        """
        CreateMultipartUpload
        CompleteMultipartUpload
        """
        uploads = request.query_params.get('uploads', None)
        if uploads is not None:
            return self.create_multipart_upload(request=request, args=args, kwargs=kwargs)

        upload_id = request.query_params.get('uploadId', None)
        if upload_id is not None:
            return self.complete_multipart_upload(request=request, upload_id=upload_id)

        return self.exception_response(request, exceptions.S3MethodNotAllowed())

    def update(self, request, *args, **kwargs):
        """
        put object
        create dir
        upload part
        """
        key = self.get_s3_obj_key(request)
        content_length = request.headers.get('Content-Length', None)
        if not content_length:
            return self.exception_response(request, exceptions.S3MissingContentLength())

        if key.endswith('/') and content_length == '0':
            return self.create_dir(request=request, args=args, kwargs=kwargs)

        # UploadPart check
        part_num = request.query_params.get('partNumber', None)
        upload_id = request.query_params.get('uploadId', None)
        if part_num is not None and upload_id is not None:
            if 'x-amz-copy-source-range' in request.headers or 'x-amz-copy-source' in request.headers:
                return self.exception_response(request, exceptions.S3NotImplemented(
                    message='UploadPartCopy not implemented'))

            return self.upload_part(request=request, part_num=part_num, upload_id=upload_id)

        # PutObjectAcl
        if 'acl' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectAcl not implemented'))

        # PutObjectLegalHold
        if 'legal-hold' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectLegalHold not implemented'))

        # PutObjectLockConfiguration
        if 'object-lock' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectLockConfiguration not implemented'))

        # PutObjectRetention
        if 'retention' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectRetention not implemented'))

        # PutObjectTagging
        if 'tagging' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectTagging not implemented'))

        return self.put_or_copy_oject(request, args, kwargs)

    def destroy(self, request, *args, **kwargs):
        """
        delete object
        delete dir
        AbortMultipartUpload
        """
        upload_id = request.query_params.get('uploadId', None)
        if upload_id is not None:
            return self.abort_multipart_upload(request=request, upload_id=upload_id)

        key = self.get_s3_obj_key(request)
        if key.endswith('/'):
            return self.delete_dir(request=request, args=args, kwargs=kwargs)

        return self.delete_object(request=request, args=args, kwargs=kwargs)

    def head(self, request, *args, **kwargs):
        """
        head object
        """
        return self.head_object(request=request, args=args, kwargs=kwargs)

    def s3_get_object(self, request, args, kwargs):
        bucket_name = self.get_bucket_name(request)
        s3_obj_key = self.get_s3_obj_key(request)
        obj_path_name = s3_obj_key.strip('/')

        part_number = request.query_params.get('partNumber', None)
        header_range = request.headers.get('range', None)
        if part_number is not None and header_range is not None:
            return self.exception_response(request, exceptions.S3InvalidRequest())

        # 存储桶验证和获取桶对象
        hm = HarborManager()
        try:
            bucket, fileobj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=obj_path_name,
                                                           user=request.user, all_public=True)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if fileobj is None:
            return self.exception_response(request, exceptions.S3NoSuchKey())

        if s3_obj_key.endswith('/'):  # dir
            if fileobj.is_file():
                return self.exception_response(request, exceptions.S3NoSuchKey())

            is_dir = True
        else:                          # object
            if not fileobj.is_file():
                return self.exception_response(request, exceptions.S3NoSuchKey())

            is_dir = False

        # 是否有文件对象的访问权限
        try:
            self.has_object_access_permission(request=request, bucket=bucket, obj=fileobj)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if is_dir:
            if part_number is not None and part_number != '1':
                return self.exception_response(request,
                                               exceptions.S3InvalidArgument(message=gettext('无效的参数partNumber.')))

            response = self.s3_get_object_dir(fileobj)
        elif part_number is not None:
            try:
                part_number = int(part_number)
                response = self.s3_get_object_part_response(bucket=bucket, obj=fileobj, part_number=part_number)
            except ValueError:
                return self.exception_response(request, exceptions.S3InvalidArgument(message=gettext('无效的参数partNumber.')))
            except exceptions.S3Error as e:
                return self.exception_response(request, e)
        else:
            try:
                response = self.s3_get_object_range_or_whole_response(request=request, bucket=bucket, obj=fileobj)
            except exceptions.S3Error as e:
                return self.exception_response(request, e)

        upt = fileobj.upt if fileobj.upt else fileobj.ult
        etag = response['ETag']
        try:
            self.head_object_precondition_if_headers(request, obj_upt=upt, etag=etag)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        # 用户设置的参数覆盖
        response_content_disposition = request.query_params.get('response-content-disposition', None)
        response_content_type = request.query_params.get('response-content-type', None)
        response_content_encoding = request.query_params.get('response-content-encoding', None)
        response_content_language = request.query_params.get('response-content-language', None)
        if response_content_disposition:
            response['Content-Disposition'] = response_content_disposition
        if response_content_encoding:
            response['Content-Encoding'] = response_content_encoding
        if response_content_language:
            response['Content-Language'] = response_content_language
        if response_content_type:
            response['Content-Type'] = response_content_type

        response['x-amz-storage-class'] = 'STANDARD'

        return response

    @staticmethod
    def get_object_part(bucket, obj_id: int, part_number: int):
        """
        获取对象一个part元数据

        :return:
            part
            None    # part_number == 1时，非多部分对象

        :raises: S3Error
        """
        if not (1 <= part_number <= 10000):
            raise exceptions.S3InvalidPartNumber()

        opm = ObjectPartManager(bucket=bucket)
        part = opm.get_part_by_obj_id_part_num(obj_id=obj_id, part_num=part_number)
        if part:
            return part

        if part_number == 1:
            return None

        raise exceptions.S3InvalidPartNumber()

    def s3_get_object_part_response(self, bucket, obj, part_number: int):
        """
        读取对象一个part的响应

        :return:
            Response()

        :raises: S3Error
        """
        obj_size = obj.si
        part = self.get_object_part(bucket=bucket, obj_id=obj.id, part_number=part_number)
        if part:
            offset = part.obj_offset
            size = part.size
            end = offset + size - 1
            generator = HarborManager()._get_obj_generator(bucket=bucket, obj=obj, offset=offset, end=end)
            response = FileResponse(generator, status=status.HTTP_206_PARTIAL_CONTENT)
            response['Content-Length'] = end - offset + 1
            response['ETag'] = part.obj_etag
            response['x-amz-mp-parts-count'] = part.parts_count
            response['Content-Range'] = f'bytes {offset}-{end}/{obj_size}'
        else:   # 非多部分对象
            generator = HarborManager()._get_obj_generator(bucket=bucket, obj=obj)
            response = FileResponse(generator)
            response['Content-Length'] = obj_size
            response['ETag'] = obj.md5
            if obj_size > 0:
                end = max(obj_size - 1, 0)
                response['Content-Range'] = f'bytes {0}-{end}/{obj_size}'

        last_modified = obj.upt if obj.upt else obj.ult
        filename = urlquote(obj.name)  # 中文文件名需要
        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'binary/octet-stream'  # 注意格式
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字

        return response

    def s3_get_object_range_or_whole_response(self, request, bucket, obj):
        """
        读取对象指定范围或整个对象

        :return:
            Response()

        :raises: S3Error
        """
        obj_size = obj.si
        filename = obj.name
        hm = HarborManager()
        ranges = request.headers.get('range', None)
        if ranges is not None:  # 是否是断点续传部分读取
            offset, end = self.get_object_offset_and_end(ranges, filesize=obj_size)

            generator = hm._get_obj_generator(bucket=bucket, obj=obj, offset=offset, end=end)
            response = FileResponse(generator, status=status.HTTP_206_PARTIAL_CONTENT)
            response['Content-Range'] = f'bytes {offset}-{end}/{obj_size}'
            response['Content-Length'] = end - offset + 1
        else:
            generator = hm._get_obj_generator(bucket=bucket, obj=obj)
            response = FileResponse(generator)
            response['Content-Length'] = obj_size

            # 增加一次下载次数
            obj.download_cound_increase()

        # multipart object check
        parts_qs = ObjectPartManager(bucket=bucket).get_parts_queryset_by_obj_id(obj_id=obj.id)
        part = parts_qs.first()
        if part:        #
            response['ETag'] = part.obj_etag
            response['x-amz-mp-parts-count'] = part.parts_count
        else:
            response['ETag'] = obj.md5

        last_modified = obj.upt if obj.upt else obj.ult
        filename = urlquote(filename)  # 中文文件名需要

        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'binary/octet-stream'  # 注意格式
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字
        return response

    @staticmethod
    def s3_get_object_dir(obj):
        """
        获取的是一个目录
        :return:
            Response()
        """
        last_modified = obj.upt if obj.upt else obj.ult
        response = FileResponse(b'')
        response['Content-Length'] = 0
        response['ETag'] = f'"{EMPTY_HEX_MD5}"'
        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'application/x-directory; charset=UTF-8'  # 注意格式, dir
        return response

    def get_object_offset_and_end(self, h_range: str, filesize: int):
        """
        获取读取开始偏移量和结束偏移量

        :param h_range: range Header
        :param filesize: 对象大小
        :return:
            (offset:int, end:int)

        :raise S3Error
        """
        start, end = self.parse_header_range(h_range)
        if start is None and end is None:
            raise exceptions.S3InvalidRange()

        if isinstance(start, int):
            if start >= filesize or start < 0:
                raise exceptions.S3InvalidRange()

        end_max = filesize - 1
        # 读最后end个字节
        if (start is None) and isinstance(end, int):
            offset = max(filesize - end, 0)
            end = end_max
        else:
            offset = start
            if isinstance(end, int):
                end = min(end, end_max)
            else:
                end = end_max

        return offset, end

    @staticmethod
    def parse_header_range(h_range: str):
        """
        parse Range header string

        :param h_range: 'bytes={start}-{end}'  下载第M－N字节范围的内容
        :return: (M, N)
            start: int or None
            end: int or None
        """
        m = re.match(r'bytes=(\d*)-(\d*)', h_range)
        if not m:
            return None, None
        items = m.groups()

        start = int(items[0]) if items[0] else None
        end = int(items[1]) if items[1] else None
        if isinstance(start, int) and isinstance(end, int) and start > end:
            return None, None
        return start, end

    @staticmethod
    def has_object_access_permission(request, bucket, obj):
        """
        当前已认证用户或未认证用户是否有访问对象的权限

        :param request: 请求体对象
        :param bucket: 存储桶对象
        :param obj: 文件对象
        :return:
            True(可访问)
            raise S3AccessDenied  # 不可访问

        :raises: S3AccessDenied
        """
        # 存储桶是否是公有权限
        if bucket.is_public_permission():
            return True

        # 存储桶是否属于当前用户
        if bucket.check_user_own_bucket(request.user):
            return True

        # 对象是否共享的，并且在有效共享事件内
        if not obj.is_shared_and_in_shared_time():
            raise exceptions.S3AccessDenied(message=gettext('您没有访问权限'))

        # 是否设置了分享密码
        if obj.has_share_password():
            p = request.query_params.get('p', None)
            if p is None:
                raise exceptions.S3AccessDenied(message=gettext('资源设有共享密码访问权限'))
            if not obj.check_share_password(password=p):
                raise exceptions.S3AccessDenied(message=gettext('共享密码无效'))

        return True

    def create_object_metadata(self, request):
        bucket_name = self.get_bucket_name(request)
        obj_path_name = self.get_obj_path_name(request)

        # 访问权限
        acl_choices = {'private': BucketFileBase.SHARE_ACCESS_NO, 'public-read': BucketFileBase.SHARE_ACCESS_READONLY,
                       'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE}
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()
        if x_amz_acl not in acl_choices:
            raise exceptions.S3InvalidRequest(f'The value {x_amz_acl} of header "x-amz-acl" is not supported.')

        h_manager = HarborManager()
        bucket, obj, created = h_manager.create_empty_obj(bucket_name=bucket_name, obj_path=obj_path_name,
                                                          user=request.user)

        if x_amz_acl != 'private':
            share_code = acl_choices[x_amz_acl]
            obj.set_shared(share=share_code)

        pool_name = bucket.get_pool_name()
        obj_key = obj.get_obj_key(bucket.id)

        rados = HarborObject(pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
        if created is False:  # 对象已存在，不是新建的
            try:
                h_manager._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)  # 重置对象大小
            except Exception as exc:
                raise exceptions.S3InvalidRequest(f'reset object error, {str(exc)}')

        return bucket, obj, rados, created

    def put_or_copy_oject(self, request, args, kwargs):
        if 'x-amz-copy-source' in request.headers:
            return handlers.CopyObjectHandler().copy_object(request=request, view=self)

        return self.put_object(request=request, args=args, kwargs=kwargs)

    def put_object(self, request, args, kwargs):
        try:
            bucket, obj, rados, created = self.create_object_metadata(request=request)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        return self.put_object_handle(request=request, bucket=bucket, obj=obj, rados=rados, created=created)

    def put_object_handle(self, request, bucket, obj, rados, created):
        pool_name = bucket.get_pool_name()
        obj_key = obj.get_obj_key(bucket.id)
        uploader = FileUploadToCephHandler(request, pool_name=pool_name, obj_key=obj_key)
        request.upload_handlers = [uploader]

        def clean_put(_uploader, _obj, _created):
            # 删除数据和元数据
            f = getattr(_uploader, 'file', None)
            s = f.size if f else 0
            try:
                rados.delete(obj_size=s)
            except Exception:
                pass
            if _created:
                _obj.do_delete()

        try:
            self.kwargs['filename'] = 'filename'
            put_data = request.data
        except UnsupportedMediaType:
            clean_put(uploader, obj, created)
            return self.exception_response(request, exceptions.S3UnsupportedMediaType())
        except RadosError as e:
            clean_put(uploader, obj, created)
            return self.exception_response(request, exceptions.S3InternalError(extend_msg=str(e)))
        except Exception as exc:
            clean_put(uploader, obj, created)
            return self.exception_response(request, exceptions.S3InvalidRequest(extend_msg=str(exc)))

        file = put_data.get('file')
        if not file:
            content_length = self.request.headers.get('Content-Length', None)
            try:
                content_length = int(content_length)
            except Exception:
                clean_put(uploader, obj, created)
                return self.exception_response(request, exceptions.S3MissingContentLength())

            # 是否是空对象
            if content_length != 0:
                clean_put(uploader, obj, created)
                return self.exception_response(request, exceptions.S3InvalidRequest('Request body is empty.'))

            bytes_md5 = EMPTY_BYTES_MD5
            obj_md5 = EMPTY_HEX_MD5
            obj_size = 0
        else:
            bytes_md5 = file.md5_handler.digest()
            obj_md5 = file.file_md5
            obj_size = file.size

        content_b64_md5 = self.request.headers.get('Content-MD5', '')
        if content_b64_md5:
            base64_md5 = base64.b64encode(bytes_md5).decode('ascii')
            if content_b64_md5 != base64_md5:
                # 删除数据和元数据
                clean_put(uploader, obj, created)
                return self.exception_response(request, exceptions.S3BadDigest())

        try:
            obj.si = obj_size
            obj.md5 = obj_md5
            obj.save(update_fields=['si', 'md5'])
        except Exception as e:
            # 删除数据和元数据
            clean_put(uploader, obj, created)
            return self.exception_response(request, exceptions.S3InternalError('更新对象元数据错误'))

        headers = {'ETag': obj_md5}
        x_amz_acl = request.headers.get('x-amz-acl', None)
        if x_amz_acl:
            headers['X-Amz-Acl'] = x_amz_acl
        return Response(status=status.HTTP_200_OK, headers=headers)

    def delete_object(self, request, args, kwargs):
        bucket_name = self.get_bucket_name(request)
        obj_path_name = self.get_obj_path_name(request)
        h_manager = HarborManager()
        try:
            h_manager.delete_object(bucket_name=bucket_name, obj_path=obj_path_name, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def create_dir(self, request, args, kwargs):
        bucket_name = self.get_bucket_name(request)
        dir_path_name = self.get_obj_path_name(request)

        if not dir_path_name:
            return self.exception_response(request, exceptions.S3InvalidSuchKey())

        content_length = self.request.headers.get('Content-Length', None)
        if content_length is None:
            return self.exception_response(request, exceptions.S3MissingContentLength())

        try:
            content_length = int(content_length)
        except Exception:
            return self.exception_response(request, exceptions.S3InvalidContentLength())

        if content_length != 0:
            return self.exception_response(request, exceptions.S3InvalidContentLength())

        hm = HarborManager()
        bucket = hm.get_user_own_bucket(name=bucket_name, user=request.user)
        if not bucket:
            return self.exception_response(request, exceptions.S3NoSuchBucket())

        table_name = bucket.get_bucket_table_name()
        try:
            hm.create_path(table_name=table_name, path=dir_path_name)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        return Response(status=status.HTTP_200_OK, headers={'ETag': EMPTY_HEX_MD5})

    def delete_dir(self, request, args, kwargs):
        bucket_name = self.get_bucket_name(request)
        dir_path_name = self.get_obj_path_name(request)
        if not dir_path_name:
            return self.exception_response(request, exceptions.S3InvalidSuchKey())

        try:
            HarborManager().rmdir(bucket_name=bucket_name, dirpath=dir_path_name, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def get_parsers(self):
        """
        动态分配请求体解析器
        """
        method = self.request.method.lower()
        if method == 'put':                     # put_object
            return [FileUploadParser()]

        return super().get_parsers()

    def create_multipart_upload(self, request, *args, **kwargs):
        bucket_name = self.get_bucket_name(request)
        obj_path_name = self.get_obj_path_name(request)
        expires = request.headers.get('Expires', None)
        expires_time = None
        if expires:     # 对象不再存储（删除）的时间
            expires_time = datetime_from_gmt(expires)
            if expires_time is None:
                return self.exception_response(request, exceptions.S3InvalidArgument("Expires is invalid GMT datetime"))

        # 访问权限
        acl_choices = {'private': BucketFileBase.SHARE_ACCESS_NO, 'public-read': BucketFileBase.SHARE_ACCESS_READONLY,
                       'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE}
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()
        if x_amz_acl not in acl_choices:
            raise exceptions.S3InvalidRequest(f'The value {x_amz_acl} of header "x-amz-acl" is not supported.')

        h_manager = HarborManager()
        try:
            bucket = h_manager.get_public_or_user_bucket(name=bucket_name, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if not bucket.is_s3_bucket():
            return self.exception_response(request, exceptions.S3NotS3Bucket())

        mu_mgr = MultipartUploadManager()
        try:
            upload = mu_mgr.get_multipart_upload_delete_invalid(bucket=bucket, obj_path=obj_path_name)
            if upload and upload.is_composing():   # 正在组合对象，不允许操作
                # return self.exception_response(request, exceptions.S3CompleteMultipartAlreadyInProgress())
                upload.set_uploading()
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        obj_table_name = bucket.get_bucket_table_name()
        ok = h_manager.ensure_path_and_no_same_name_dir(table_name=obj_table_name, obj_path_name=obj_path_name)

        obj_perms_code = acl_choices[x_amz_acl]
        if upload:
            if upload.is_completed():       # 存在已完成的上传任务记录，删除
                upload.safr_delete()
                upload = None
            else:
                mu_mgr.update_upload_belong_to_object(upload=upload, bucket=bucket, obj_key=obj_path_name,
                                                      obj_perms_code=obj_perms_code, expire_time=expires_time)

        if not upload:
            upload = mu_mgr.create_multipart_upload_task(bucket=bucket, obj_key=obj_path_name,
                                                         obj_perms_code=obj_perms_code, expire_time=expires_time)

        data = {
            'Bucket': bucket.name,
            'Key': obj_path_name,
            'UploadId': upload.id
        }
        self.set_renderer(request, renders.CusXMLRenderer(root_tag_name='InitiateMultipartUploadResult'))
        return Response(data=data, status=status.HTTP_200_OK)

    def upload_part(self, request, part_num: str, upload_id: str):
        """
        multipart upload part
        """
        bucket_name = self.get_bucket_name(request)
        content_length = request.headers.get('Content-Length', 0)
        try:
            content_length = int(content_length)
        except ValueError:
            return self.exception_response(request, exceptions.S3InvalidContentLength())

        if content_length == 0:
            return self.exception_response(request, exceptions.S3EntityTooSmall())

        if content_length > MULTIPART_UPLOAD_MAX_SIZE:
            return self.exception_response(request, exceptions.S3EntityTooLarge())

        try:
            part_num = int(part_num)
        except ValueError:
            return self.exception_response(request, exceptions.S3InvalidArgument('Invalid param PartNumber'))

        if not (0 < part_num <= 10000):
            return self.exception_response(request, exceptions.S3InvalidArgument(
                'Invalid param PartNumber, must be a positive integer between 1 and 10,000.'))

        try:
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id, bucket_name=bucket_name)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if not bucket.is_s3_bucket():
            return self.exception_response(request, exceptions.S3NotS3Bucket())

        if upload.is_composing():  # 正在组合对象，不允许操作
            return self.exception_response(request, exceptions.S3CompleteMultipartAlreadyInProgress())

        return self.upload_part_handle(request=request, bucket=bucket, upload=upload, part_number=part_num)

    def upload_part_handle(self, request, bucket, upload, part_number: int):
        """
        :raises: S3Error,  Exception
        """
        part_key = build_part_rados_key(upload_id=upload.id, part_num=part_number)
        uploader = PartUploadToCephHandler(request, part_key=part_key)
        request.upload_handlers = [uploader]

        def clean_put(_uploader):
            # 删除数据
            f = getattr(_uploader, 'file', None)
            if f is not None:
                try:
                    f.delete()
                except Exception:
                    pass

        try:
            part = self.upload_part_handle_save(request=request, bucket=bucket, upload=upload, part_number=part_number)
        except exceptions.S3Error as e:
            clean_put(uploader)
            return self.exception_response(request, e)
        except Exception as exc:
            clean_put(uploader)
            return self.exception_response(request, exceptions.S3InvalidRequest(extend_msg=str(exc)))

        return Response(status=status.HTTP_200_OK, headers={'ETag': f'"{part.part_md5}"'})

    def upload_part_handle_save(self, request, bucket, upload, part_number: int):
        """
        :raises: S3Error
        """
        parts_table_name = bucket.get_parts_table_name()
        op_mgr = ObjectPartManager(parts_table_name=parts_table_name)

        try:
            self.kwargs['filename'] = 'filename'
            put_data = request.data
        except UnsupportedMediaType:
            raise exceptions.S3UnsupportedMediaType()
        except RadosError as e:
            raise exceptions.S3InternalError(extend_msg=str(e))
        except Exception as exc:
            raise exceptions.S3InvalidRequest(extend_msg=str(exc))

        file = put_data.get('file')
        if not file:
            raise exceptions.S3InvalidRequest('Request body is empty.')

        part_md5 = file.file_md5
        part_size = file.size

        amz_content_sha256 = self.request.headers.get('X-Amz-Content-SHA256', None)
        if amz_content_sha256 is None:
            raise exceptions.S3InvalidContentSha256Digest()

        if amz_content_sha256 != 'UNSIGNED-PAYLOAD':
            part_sha256 = file.sha256_handler.hexdigest()
            if amz_content_sha256 != part_sha256:
                raise exceptions.S3BadContentSha256Digest()

        part = op_mgr.get_part_by_upload_id_part_num(upload_id=upload.id, part_num=part_number)
        if part:
            part.size = part_size
            part.part_md5 = part_md5
            part.upload_id = upload.id
            part.obj_id = 0             # 未组合对象的part，默认为0， 组合后为对象id
            try:
                part.save(update_fields=['size', 'part_md5', 'upload_id', 'modified_time'])
            except Exception as e:
                raise exceptions.S3InternalError('更新对象元数据错误')
        else:
            part = op_mgr.create_part_metadata(upload_id=upload.id, obj_id=0, part_num=part_number,
                                               size=part_size, part_md5=part_md5)

        return part

    def get_upload_and_bucket(self, request, upload_id: str, bucket_name: str):
        """
        :return:
            upload, bucket
        :raises: S3Error
        """
        obj_path_name = self.get_s3_obj_key(request)

        mu_mgr = MultipartUploadManager()
        upload = mu_mgr.get_multipart_upload_by_id(upload_id=upload_id)

        if not upload:
            raise exceptions.S3NoSuchUpload()

        if upload.obj_key != obj_path_name:
            raise exceptions.S3NoSuchUpload(f'UploadId conflicts with this object key.Please Key "{upload.obj_key}"')

        hm = HarborManager()
        bucket = hm.get_user_own_bucket(name=bucket_name, user=request.user)
        if not bucket:
            raise exceptions.S3NoSuchBucket()

        if not upload.belong_to_bucket(bucket):
            raise exceptions.S3NoSuchUpload(f'UploadId conflicts with this bucket.'
                f'Please bucket "{bucket.name}".Maybe the UploadId is created for deleted bucket.')

        return upload, bucket

    @staticmethod
    def handle_validate_complete_parts(parts: list):
        """
        检查对象part列表是否是升序排列, 是否有效（1-10000）
        :return: parts_dict, numbers
                parts_dict: dict, {PartNumber: parts[index]} 把parts列表转为以PartNumber为键值的有序字典
                numbers: list, [PartNumber, PartNumber]

        :raises: S3Error
        """
        pre_num = 0
        numbers = []
        parts_dict = OrderedDict()
        for part in parts:
            part_num = part.get('PartNumber', None)
            etag = part.get('ETag', None)
            if part_num is None or etag is None:
                raise exceptions.S3MalformedXML()

            if not (1 <= part_num <= 10000):
                raise exceptions.S3InvalidPart()

            if part_num <= pre_num:
                raise exceptions.S3InvalidPartOrder()

            parts_dict[part_num] = part
            numbers.append(part_num)
            pre_num = part_num

        return parts_dict, numbers

    def complete_multipart_upload(self, request, upload_id: str):
        bucket_name = self.get_bucket_name(request)
        obj_path_name = self.get_s3_obj_key(request)

        if not upload_id:
            return self.exception_response(request, exceptions.S3NoSuchUpload())

        try:
            data = request.data
        except Exception as e:
            return self.exception_response(request, exceptions.S3MalformedXML())

        root = data.get('CompleteMultipartUpload')
        if not root:
            return self.exception_response(request, exceptions.S3MalformedXML())

        complete_parts_list = root.get('Part')
        if not complete_parts_list:
            return self.exception_response(request, exceptions.S3MalformedXML())

        # XML解析器行为有关，只有一个part时不是list
        if not isinstance(complete_parts_list, list):
            complete_parts_list = [complete_parts_list]

        complete_parts_dict, complete_part_numbers = self.handle_validate_complete_parts(complete_parts_list)

        try:
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id, bucket_name=bucket_name)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        return handlers.MultipartUploadHandler().complete_multipart_upload_handle(
            request=request, bucket=bucket, upload=upload, key=obj_path_name, complete_parts=complete_parts_dict,
            complete_numbers=complete_part_numbers)

    def abort_multipart_upload(self, request, upload_id: str):
        bucket_name = self.get_bucket_name(request)

        if not upload_id:
            return self.exception_response(request, exceptions.S3NoSuchUpload())

        try:
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id, bucket_name=bucket_name)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        return handlers.MultipartUploadHandler().abort_multipart_upload(request=request, upload=upload, bucket=bucket)

    def head_object(self, request, *args, **kwargs):
        bucket_name = self.get_bucket_name(request)
        obj_path_name = self.get_obj_path_name(request)
        part_number = request.query_params.get('partNumber', None)
        ranges = request.headers.get('range', None)

        if ranges is not None and part_number is not None:
            return self.exception_response(request, exceptions.S3InvalidRequest())

        # 存储桶验证和获取桶对象
        hm = HarborManager()
        try:
            bucket, fileobj = hm.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path_name,
                                                    user=request.user, all_public=True)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if fileobj is None:
            return self.exception_response(request, exceptions.S3NoSuchKey())

        # 是否有文件对象的访问权限
        try:
            self.has_object_access_permission(request=request, bucket=bucket, obj=fileobj)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if part_number is not None or ranges is not None:
            if part_number:
                try:
                    part_number = int(part_number)
                except ValueError:
                    return self.exception_response(request, exceptions.S3InvalidArgument(message=gettext('无效的参数partNumber.')))

            try:
                response = self.head_object_part_or_range_response(bucket=bucket, obj=fileobj, part_number=part_number,
                                                                   header_range=ranges)
            except exceptions.S3Error as e:
                return self.exception_response(request, e)
        else:
            try:
                response = self.head_object_common_response(bucket=bucket, obj=fileobj)
            except exceptions.S3Error as e:
                return self.exception_response(request, e)

        upt = fileobj.upt if fileobj.upt else fileobj.ult
        etag = response['ETag']
        try:
            self.head_object_precondition_if_headers(request, obj_upt=upt, etag=etag)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        # 防止标头Content-Type被渲染器覆盖
        response.content_type = response['Content-Type'] if response.has_header('Content-Type') else None
        return response

    def head_object_no_multipart_response(self, obj, status_code: int = 200, headers=None):
        """
        非多部分对象head响应
        """
        h = self.head_object_common_headers(obj=obj)
        if headers:
            h.update(headers)
        return Response(status=status_code, headers=h)

    def head_object_common_response(self, bucket, obj):
        """
        对象head响应，会检测对象是否是多部分对象
        :raises: S3Error
        """
        # multipart object check
        parts_qs = ObjectPartManager(bucket=bucket).get_parts_queryset_by_obj_id(obj_id=obj.id)
        part = parts_qs.first()
        headers = self.head_object_common_headers(obj=obj, part=part)

        return Response(status=status.HTTP_200_OK, headers=headers)

    @staticmethod
    def head_object_common_headers(obj, part=None):
        last_modified = obj.upt if obj.upt else obj.ult
        headers = {
            'Content-Length': obj.si,
            'Last-Modified': serializers.time_to_gmt(last_modified),
            'Accept-Ranges': 'bytes',  # 接受类型，支持断点续传
            'Content-Type': 'binary/octet-stream'
        }

        if part:
            headers['ETag'] = part.obj_etag
            headers['x-amz-mp-parts-count'] = part.parts_count
        else:
            headers['ETag'] = obj.md5

        return headers

    def head_object_part_or_range_response(self, bucket, obj, part_number: int, header_range: str):
        """
        head对象指定部分编号或byte范围

        :param bucket: 桶实例
        :param obj: 对象元数据实例
        :param part_number: int or None
        :param header_range: str or None
        :return:

        :raises: S3Error
        """
        obj_size = obj.si
        response = Response(status=status.HTTP_206_PARTIAL_CONTENT)

        if header_range:
            offset, end = self.get_object_offset_and_end(header_range, filesize=obj_size)

            # multipart object check
            parts_qs = ObjectPartManager(bucket=bucket).get_parts_queryset_by_obj_id(obj_id=obj.id)
            part = parts_qs.first()
            if part:
                response['ETag'] = part.obj_etag
                response['x-amz-mp-parts-count'] = part.parts_count
            else:
                response['ETag'] = obj.md5
        elif part_number:
            part = self.get_object_part(bucket=bucket, obj_id=obj.id, part_number=part_number)
            if not part:
                content_range = f'bytes 0-{obj_size-1}/{obj_size}'
                return self.head_object_no_multipart_response(obj, status_code=status.HTTP_206_PARTIAL_CONTENT,
                                                              headers={'Content-Range': content_range})
            response['ETag'] = part.obj_etag
            response['x-amz-mp-parts-count'] = part.parts_count
            offset = part.obj_offset
            size = part.size
            end = offset + size - 1
        else:
            raise exceptions.S3InvalidRequest()

        last_modified = obj.upt if obj.upt else obj.ult
        response['Content-Range'] = f'bytes {offset}-{end}/{obj_size}'
        response['Content-Length'] = end - offset + 1
        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'binary/octet-stream'  # 注意格式
        return response

    @staticmethod
    def head_object_precondition_if_headers(request, obj_upt, etag: str):
        """
        标头if条件检查

        :param request:
        :param obj_upt: 对象最后修改时间
        :param etag: 对象etag
        :return: None
        :raises: S3Error
        """
        handlers.check_precondition_if_headers(
            headers=request.headers, last_modified=obj_upt, etag=etag,
            key_match='If-Match',
            key_none_match='If-None-Match',
            key_modified_since='If-Modified-Since',
            key_unmodified_since='If-Unmodified-Since'
        )
