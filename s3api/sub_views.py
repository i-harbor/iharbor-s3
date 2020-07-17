import base64
import re
from collections import OrderedDict

from django.utils.translation import gettext as _
from django.http import FileResponse, QueryDict
from django.utils.http import urlquote
from django.conf import settings
from django.utils import timezone
from rest_framework.response import Response
from rest_framework import status
from rest_framework.serializers import ValidationError
from rest_framework.exceptions import UnsupportedMediaType
from rest_framework.parsers import FileUploadParser

from buckets.models import Bucket
from . import renders
from .viewsets import CustomGenericViewSet
from .validators import DNSStringValidator, bucket_limit_validator
from .utils import (get_ceph_poolname_rand, BucketFileManagement, create_table_for_model_class,
                    delete_table_for_model_class)
from . import exceptions
from .harbor import HarborManager
from utils.storagers import FileUploadToCephHandler, PartUploadToCephHandler
from utils.md5 import EMPTY_BYTES_MD5, EMPTY_HEX_MD5, FileMD5Handler, S3ObjectMultipartETagHandler
from utils.oss.pyrados import HarborObject, RadosError, ObjectPart
from buckets.models import BucketFileBase
from . import serializers
from . import paginations
from .managers import (get_parts_model_class, MultipartUploadManager, ObjectPartManager)
from .negotiation import CusContentNegotiation
from . import parsers
from .models import build_part_rados_key


MULTIPART_UPLOAD_MAX_SIZE = getattr(settings, 'S3_MULTIPART_UPLOAD_MAX_SIZE', 2 * 1024 ** 3)        # default 2GB
MULTIPART_UPLOAD_MIN_SIZE = getattr(settings, 'S3_MULTIPART_UPLOAD_MIN_SIZE', 5 * 1024 ** 2)        # default 5MB


class BucketViewSet(CustomGenericViewSet):
    http_method_names = ['get', 'put', 'delete', 'head', 'options']
    renderer_classes = [renders.CusXMLRenderer]

    def list(self, request, *args, **kwargs):
        """
        list objects (v1 && v2)
        get object metadata
        """
        list_type = request.query_params.get('list-type', '1')
        if list_type == '2':
            return self.list_objects_v2(request=request, args=args, kwargs=kwargs)

        return self.list_objects_v1(request=request, args=args, kwargs=kwargs)

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

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket_name:
            return self.exception_response(request, exceptions.S3NoSuchKey('Invalid request domain name'))

        if not bucket.check_user_own_bucket(user=request.user):
            return self.exception_response(request, exceptions.S3AccessDenied())

        if not bucket.delete_and_archive():  # 删除归档
            return self.exception_response(request, exceptions.S3InternalError(_('删除存储桶失败')))

        return Response(status=status.HTTP_204_NO_CONTENT)

    def head_bucket(self, request, *args, **kwargs):
        bucket_name = self.get_bucket_name(request)
        if not bucket_name:
            return self.exception_response(request, exceptions.S3InvalidRequest('Invalid request domain name'))

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket_name:
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
        bucket = Bucket(pool_name=pool_name, user=user, name=bucket_name, access_permission=perms)
        try:
            bucket.save()
        except Exception as e:
            return self.exception_response(request, exceptions.S3InternalError(message=_('创建存储桶失败，存储桶元数据错误')))

        col_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=col_name)
        model_class = bfm.get_obj_model_class()
        if not create_table_for_model_class(model=model_class):
            bucket.delete()
            delete_table_for_model_class(model=model_class)
            return self.exception_response(request, exceptions.S3InternalError(message=_('创建存储桶失败，存储桶object表错误')))

        part_table_name = bucket.get_parts_table_name()
        parts_class = get_parts_model_class(table_name=part_table_name)
        if not create_table_for_model_class(model=parts_class):
            bucket.delete()
            delete_table_for_model_class(model=parts_class)
            delete_table_for_model_class(model=model_class)
            return self.exception_response(request, exceptions.S3InternalError(message=_('创建存储桶失败，存储桶parts表错误')))

        return Response(status=status.HTTP_200_OK)

    def list_objects_v2(self, request, *args, **kwargs):
        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', '')
        fetch_owner = request.query_params.get('fetch-owner', '').lower()
        bucket_name = self.get_bucket_name(request)

        if not delimiter:    # list所有对象和目录
            return self.list_objects_v2_list_prefix(request=request, prefix=prefix)

        if delimiter != '/':
            return self.exception_response(request, exceptions.S3InvalidArgument(message=_('参数“delimiter”必须是“/”')))

        path = prefix.strip('/')
        if prefix and not path:     # prefix invalid, return no match data
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter)

        hm = HarborManager()
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if obj is None:
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter)

        paginator = paginations.ListObjectsV2CursorPagination(context={'bucket': bucket})
        max_keys = paginator.get_page_size(request=request)
        ret_data = {
            'IsTruncated': 'false',     # can not use bool
            'Name': bucket_name,
            'Prefix': prefix,
            'EncodingType': 'url',
            'MaxKeys': max_keys
        }

        if prefix == '' or prefix.endswith('/'):  # list dir
            if not obj.is_dir():
                return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter)

            objs_qs = hm.list_dir_queryset(bucket=bucket, dir_obj=obj)
            paginator.paginate_queryset(objs_qs, request=request)
            objs, _ = paginator.get_objects_and_dirs()

            if fetch_owner == 'true':
                serializer = serializers.ObjectListWithOwnerSerializer(objs, many=True, context={'user': request.user})
            else:
                serializer = serializers.ObjectListSerializer(objs, many=True)

            data = paginator.get_paginated_data(common_prefixes=True, delimiter=delimiter)
            ret_data.update(data)
            ret_data['Contents'] = serializer.data
            self.set_renderer(request, renders.ListObjectsV2XMLRenderer())
            return Response(data=ret_data, status=status.HTTP_200_OK)

        # list object metadata
        if not obj.is_file():
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter)

        if fetch_owner == 'true':
            serializer = serializers.ObjectListWithOwnerSerializer(obj, context={'user': request.user})
        else:
            serializer = serializers.ObjectListSerializer(obj)

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
            serializer = serializers.ObjectListWithOwnerSerializer(objs_dirs, many=True, context={'user': request.user})
        else:
            serializer = serializers.ObjectListSerializer(objs_dirs, many=True)

        data = paginator.get_paginated_data()
        data['Contents'] = serializer.data
        data['Name'] = bucket_name
        data['Prefix'] = prefix
        data['EncodingType'] = 'url'

        self.set_renderer(request, renders.ListObjectsV2XMLRenderer())
        return Response(data=data, status=status.HTTP_200_OK)

    def list_objects_v2_no_match(self, request, prefix, delimiter):
        bucket_name = self.get_bucket_name(request)
        paginator = paginations.ListObjectsV2CursorPagination()
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
        return self.exception_response(request, exceptions.S3InvalidArgument(_("Version v1 of ListObjects is not supported now.")))


class ObjViewSet(CustomGenericViewSet):
    http_method_names = ['get', 'post', 'put', 'delete', 'options']
    renderer_classes = [renders.CusXMLRenderer]
    content_negotiation_class = CusContentNegotiation
    parser_classes = [parsers.S3XMLParser]

    def list(self, request, *args, **kwargs):
        """
        get object
        """
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

        part_num = request.query_params.get('partNumber', None)
        upload_id = request.query_params.get('uploadId', None)
        if part_num is not None and upload_id is not None:
            return self.upload_part(request=request, part_num=part_num, upload_id=upload_id)

        return self.put_object(request, args, kwargs)

    def destroy(self, request, *args, **kwargs):
        """
        delete object
        delete dir
        """
        key = self.get_s3_obj_key(request)
        if key.endswith('/'):
            return self.delete_dir(request=request, args=args, kwargs=kwargs)

        return self.delete_object(request=request, args=args, kwargs=kwargs)

    def s3_get_object(self, request, args, kwargs):
        bucket_name = self.get_bucket_name(request)
        obj_path_name = self.get_obj_path_name(request)

        part_number = request.query_params.get('partNumber', None)
        response_content_disposition = request.query_params.get('response-content-disposition', None)
        response_content_type = request.query_params.get('response-content-type', None)
        response_content_encoding = request.query_params.get('response-content-encoding', None)
        response_content_language = request.query_params.get('response-content-language', None)

        if part_number is not None:
            return self.exception_response(request, exceptions.S3InvalidPart(message=_('暂不支持参数PartNumber')))

        # 存储桶验证和获取桶对象
        hm = HarborManager()
        try:
            bucket, fileobj = hm.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path_name)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if fileobj is None:
            return self.exception_response(request, exceptions.S3NoSuchKey())

        # 是否有文件对象的访问权限
        try:
            self.has_object_access_permission(request=request, bucket=bucket, obj=fileobj)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        filesize = fileobj.si
        filename = fileobj.name
        # 是否是断点续传部分读取
        ranges = request.headers.get('range')
        if ranges:
            try:
                offset, end = self.get_object_offset_and_end(ranges, filesize=filesize)
            except exceptions.S3Error as e:
                return self.exception_response(request, e)

            generator = hm._get_obj_generator(bucket=bucket, obj=fileobj, offset=offset, end=end)
            response = FileResponse(generator, status=status.HTTP_206_PARTIAL_CONTENT)
            response['Content-Range'] = f'bytes {offset}-{end}/{filesize}'
            response['Content-Length'] = end - offset + 1
        else:
            generator = hm._get_obj_generator(bucket=bucket, obj=fileobj)
            response = FileResponse(generator)
            response['Content-Length'] = filesize

            # 增加一次下载次数
            fileobj.download_cound_increase()

        last_modified = fileobj.upt if fileobj.upt else fileobj.ult
        filename = urlquote(filename)  # 中文文件名需要
        response['ETag'] = fileobj.md5
        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'binary/octet-stream'  # 注意格式
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字

        # 用户设置的参数覆盖
        if response_content_disposition:
            response['Content-Disposition'] = response_content_disposition
        if response_content_encoding:
            response['Content-Encoding'] = response_content_encoding
        if response_content_language:
            response['Content-Language'] = response_content_language
        if response_content_type:
            response['Content-Type'] = response_content_type

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
            raise exceptions.S3AccessDenied(message=_('您没有访问权限'))

        # 是否设置了分享密码
        if obj.has_share_password():
            p = request.query_params.get('p', None)
            if p is None:
                raise exceptions.S3AccessDenied(message=_('资源设有共享密码访问权限'))
            if not obj.check_share_password(password=p):
                raise exceptions.S3AccessDenied(message=_('共享密码无效'))

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
        parts_table_name = bucket.get_parts_table_name()

        rados = HarborObject(pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
        if created is False:  # 对象已存在，不是新建的
            try:
                h_manager._pre_reset_upload(parts_table_name=parts_table_name, obj=obj, rados=rados)  # 重置对象大小
            except Exception as exc:
                raise exceptions.S3InvalidRequest(f'reset object error, {str(exc)}')

        return bucket, obj, rados, created

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

        def clean_put(uploader, obj, created):
            # 删除数据和元数据
            f = getattr(uploader, 'file', None)
            s = f.size if f else 0
            try:
                rados.delete(obj_size=s)
            except Exception:
                pass
            if created:
                obj.do_delete()

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
        if expires:
            expires_time = serializers.datetime_from_gmt(expires)
            if expires_time is None:
                return self.exception_response(request, exceptions.S3InvalidArgument("Expires is invalid GMT datetime"))

        # 访问权限
        acl_choices = {'private': BucketFileBase.SHARE_ACCESS_NO, 'public-read': BucketFileBase.SHARE_ACCESS_READONLY,
                       'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE}
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()
        if x_amz_acl not in acl_choices:
            raise exceptions.S3InvalidRequest(f'The value {x_amz_acl} of header "x-amz-acl" is not supported.')

        h_manager = HarborManager()
        bucket = h_manager.get_bucket(bucket_name, user=request.user)
        if not bucket:
            raise exceptions.S3NoSuchBucket('存储桶不存在')

        mu_mgr = MultipartUploadManager()
        try:
            upload = mu_mgr.get_multipart_upload_delete_invalid(bucket=bucket, obj_path=obj_path_name)
            if upload and upload.is_composing():   # 正在组合对象，不允许操作
                return self.exception_response(request, exceptions.S3CompleteMultipartAlreadyInProgress())
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        obj_table_name = bucket.get_bucket_table_name()
        ok = h_manager.ensure_path_and_no_same_name_dir(table_name=obj_table_name, obj_path_name=obj_path_name)

        obj_perms_code = acl_choices[x_amz_acl]
        if upload:
            if not upload.is_completed():       # 存在已完成的上传任务记录，删除
                try:
                    upload.delete()
                except Exception as e:
                    pass

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
        self.set_renderer(request, renders.CusXMLRenderer(root_tag_name='CreateMultipartUploadOutput'))
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

        def clean_put(uploader):
            # 删除数据
            f = getattr(uploader, 'file', None)
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

        return Response(status=status.HTTP_200_OK, headers={'ETag': part.part_md5})

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

        bytes_md5 = file.md5_handler.digest()
        part_md5 = file.file_md5
        part_size = file.size

        content_b64_md5 = self.request.headers.get('Content-MD5', '')
        if not content_b64_md5:
            raise exceptions.S3InvalidDigest()

        base64_md5 = base64.b64encode(bytes_md5).decode('ascii')
        if content_b64_md5 != base64_md5:
            raise exceptions.S3BadDigest()

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

    def handle_validate_complete_parts(self, parts: list):
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

        if upload.is_completed():          # 已完成的上传任务，删除任务记录
            try:
                upload.delete()
            except Exception as e:
                pass

            return self.exception_response(request, exceptions.S3NoSuchUpload())

        if upload.is_composing():             # 已经正在组合对象，不能重复组合
            return self.exception_response(request, exceptions.S3CompleteMultipartAlreadyInProgress())

        if not upload.set_composing():      # 设置正在组合对象
            return self.exception_response(request, exceptions.S3InternalError())

        try:
            obj, etag = self.complete_multipart_upload_handle(
                request=request, bucket=bucket, upload=upload, complete_parts=complete_parts_dict,
                complete_numbers=complete_part_numbers)
        except exceptions.S3Error as e:
            upload.set_uploading()          # 发生错误，设置回正在上传
            return self.exception_response(request, e)
        except Exception as e:
            upload.set_uploading()          # 发生错误，设置回正在上传
            return self.exception_response(request, exceptions.S3InternalError())

        location = request.build_absolute_uri()
        self.set_renderer(request, renders.CusXMLRenderer(root_tag_name='CompleteMultipartUploadOutput'))
        return Response(data={'Location': location, 'Bucket': bucket.name, 'Key': obj_path_name, 'ETag': etag})

    def complete_multipart_upload_handle(self, request, bucket, upload, complete_parts, complete_numbers):
        """
        完成多部分上传处理

        :param request:
        :param bucket:
        :param upload: 多部分上传任务实例
        :param complete_parts: 请求要组合的part信息字典
        :param complete_numbers: 请求要组合的所有part的PartNumber list
        :return: obj, ETag
        :raises: S3Error
        """
        obj_path_name = self.get_s3_obj_key(request)
        hm = HarborManager()
        obj, created = hm.get_or_create_obj(table_name=bucket.get_bucket_table_name(), obj_path_name=obj_path_name)

        parts_table_name = bucket.get_parts_table_name()
        obj_raods_key = obj.get_obj_key(bucket.id)
        obj_rados = HarborObject(pool_name=bucket.pool_name, obj_id=obj_raods_key, obj_size=obj.si)
        if not created and obj.si != 0:     # 已存在的非空对象
            try:
                hm._pre_reset_upload(parts_table_name=parts_table_name, obj=obj, rados=obj_rados)  # 重置对象大小
            except Exception as exc:
                raise exceptions.S3InvalidRequest(f'reset object error, {str(exc)}')

        # 获取需要组合的所有part元数据和对象ETag，和没有用到的part元数据列表
        used_upload_parts, unused_upload_parts, obj_etag = self.get_upload_parts_and_validate(bucket=bucket,
            upload=upload, complete_parts=complete_parts, complete_numbers=complete_numbers)

        # 所有part rados数据组合对象rados
        md5_handler = FileMD5Handler()
        offset = 0
        for num in complete_numbers:
            part = used_upload_parts[num]
            self.save_part_to_object(obj=obj, obj_rados=obj_rados, offset=offset, part=part,
                                     md5_handler=md5_handler, obj_etag=obj_etag)

            offset = offset + part.size

        # 更新对象元数据
        if not self.update_obj_metedata(obj=obj, size=offset, hex_md5=md5_handler.hex_md5,
                                        share_code=upload.obj_perms_code):
            raise exceptions.S3InternalError(extend_msg='update object metadata error.')

        # 多部分上传已完成，清理数据
        self.clear_parts_cache(unused_upload_parts, is_rm_metadata=True)    # 删除无用的part元数据和rados数据
        self.clear_parts_cache(used_upload_parts, is_rm_metadata=False)     # 删除已组合的rados数据, 保留part元数据

        # 删除多部分上传upload任务
        try:
            upload.delete()
        except Exception as e:
            upload.set_completed()      # 删除失败，尝试标记已上传完成

        return obj, obj_etag

    def clear_parts_cache(self, parts, is_rm_metadata=False):
        """
        清理part缓存，part rados数据或元数据

        :param parts: part元数据实例list或dict
        :param is_rm_metadata: True(删除元数据)；False(不删元数据)
        :return:
            True
            False
        """
        if isinstance(parts, dict):
            parts = parts.values()

        part_rados = ObjectPart(part_key='', part_size=0)
        for p in parts:
            if is_rm_metadata:
                if not p.safe_delete():
                    p.safe_delete()     # 重试一次

            part_rados.reset_part_key_and_size(part_key=p.get_part_rados_key(), part_size=p.size)
            ok, _ = part_rados.delete()
            if not ok:
                part_rados.delete()     # 重试一次

        return True

    def update_obj_metedata(self, obj, size, hex_md5: str, share_code):
        """
        :return:
            True
            False
        """
        obj.si = size
        obj.md5 = hex_md5
        obj.upt = timezone.now()
        obj.share = share_code
        obj.stl = False  # 永久共享,没有共享时间限制
        try:
            obj.save(update_fields=['si', 'md5', 'upt', 'stl', 'share'])
        except Exception as e:
            return False

        return True

    def save_part_to_object(self, obj, obj_rados, offset, part, md5_handler, obj_etag: str):
        """
        把一个part数据写入对象

        :raises: S3Error
        """
        part.obj_offset = offset
        part.obj_etag = obj_etag
        part.obj_id = obj.id

        part_rados = ObjectPart(part_key=part.get_part_rados_key(), part_size=part.size)
        generator = part_rados.read_obj_generator()
        for data in generator:
            if not data:
                break

            ok, msg = obj_rados.write(offset=offset, data_block=data)
            if not ok:
                ok, msg = obj_rados.write(offset=offset, data_block=data)

            if not ok:
                raise exceptions.S3InternalError(extend_msg=msg)

            md5_handler.update(offset=offset, data=data)
            offset = offset + len(data)

        try:
            part.save(update_fields=['obj_offset', 'obj_etag', 'obj_id'])
        except Exception as e:
            raise exceptions.S3InternalError()

    def get_upload_parts_and_validate(self, bucket, upload, complete_parts, complete_numbers):
        """
        多部分上传part元数据获取和验证

        :param upload: 上传任务实例
        :param complete_parts:  客户端请求组合提交的part信息，dict
        :param complete_numbers: 客户端请求组合提交的所有part的编号list，升序
        :return:
                (
                    used_upload_parts: dict,        # complete_parts对应的part元数据实例字典
                    unused_upload_parts: list,      # 属于同一个多部分上传任务upload的，但不在complete_parts内的part元数据实例列表
                    object_etag: str                # 对象的ETag
                )
        :raises: S3Error
        """
        opm = ObjectPartManager(bucket=bucket)
        upload_parts_qs = opm.get_parts_queryset_by_upload_id(upload_id=upload.id)

        obj_etag_handler = S3ObjectMultipartETagHandler()
        used_upload_parts = {}
        unused_upload_parts = []
        last_part_number = complete_numbers[-1]
        for part in upload_parts_qs:
            num = part.part_num
            if part.part_num in complete_numbers:
                c_part = complete_parts[num]
                if part.size < MULTIPART_UPLOAD_MIN_SIZE and num != last_part_number:  # part最小限制，最后一个part除外
                    raise exceptions.S3EntityTooSmall()

                if c_part["ETag"] != part.part_md5:
                    raise exceptions.S3InvalidPart(extend_msg=f'PartNumber={num}')

                obj_etag_handler.update(part.part_md5)
                used_upload_parts[num] = part
            else:
                unused_upload_parts.append(part)

        obj_parts_count = len(used_upload_parts)
        if obj_parts_count != len(complete_parts):
            raise exceptions.S3InvalidPart()

        obj_etag = f'"{obj_etag_handler.hex_md5}-{obj_parts_count}"'
        return used_upload_parts, unused_upload_parts, obj_etag



