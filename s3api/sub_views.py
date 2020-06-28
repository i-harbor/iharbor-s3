import base64
import re

from django.utils.translation import gettext as _
from django.http import FileResponse, QueryDict
from django.utils.http import urlquote
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
from utils.storagers import FileUploadToCephHandler, EMPTY_BYTES_MD5, EMPTY_HEX_MD5
from utils.oss.pyrados import HarborObject, RadosError
from buckets.models import BucketFileBase
from . import serializers
from . import paginations


class BucketViewSet(CustomGenericViewSet):
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
            if not create_table_for_model_class(model=model_class):
                bucket.delete()
                delete_table_for_model_class(model=model_class)
                return self.exception_response(request, exceptions.S3InternalError(message=_('创建存储桶失败，存储桶表错误')))

        return Response(status=status.HTTP_200_OK)

    def list_objects_v2(self, request, *args, **kwargs):
        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', '')
        bucket_name = self.get_bucket_name(request)

        if not delimiter and not prefix:    # list所有对象和目录
            return self.list_objects_v2_list_all(request=request, prefix=prefix)

        path = prefix.strip('/')
        if prefix and not path:     # prefix invalid, return no match data
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter)

        if delimiter is None or delimiter != '/':
            e = exceptions.S3InvalidRequest(message=_('Unsupported, if param "prefix" is not empty, param "delimiter" must be "/"'))
            return self.exception_response(request, e)

        delimiter = '/'
        hm = HarborManager()
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if obj is None:
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter)

        paginator = paginations.ListObjectsV2CursorPagination()
        max_keys = paginator.get_page_size(request=request)
        ret_data = {
            'IsTruncated': 'false',     # can not use True
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
            serializer = serializers.ObjectListSerializer(objs, many=True)

            data = paginator.get_paginated_data(common_prefixes=True, delimiter=delimiter)
            ret_data.update(data)
            ret_data['Contents'] = serializer.data
            self.set_renderer(request, renders.ListObjectsV2XMLRenderer())
            return Response(data=ret_data, status=status.HTTP_200_OK)

        # list object metadata
        if not obj.is_file():
            return self.list_objects_v2_no_match(request=request, prefix=prefix, delimiter=delimiter)

        serializer = serializers.ObjectListSerializer(obj)
        ret_data['Contents'] = [serializer.data]
        ret_data['KeyCount'] = 1
        self.set_renderer(request, renders.ListObjectsV2XMLRenderer())
        return Response(data=ret_data, status=status.HTTP_200_OK)

    def list_objects_v2_list_all(self, request, prefix):
        """
        列举所有对象和目录
        """
        bucket_name = self.get_bucket_name(request)
        hm = HarborManager()
        try:
            bucket, objs_qs = hm.get_bucket_objects_dirs_queryset(bucket_name=bucket_name, user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        paginator = paginations.ListObjectsV2CursorPagination()
        objs_dirs = paginator.paginate_queryset(objs_qs, request=request)
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
    renderer_classes = [renders.CusXMLRenderer]

    def list(self, request, *args, **kwargs):
        """
        get object
        """
        return self.s3_get_object(request=request, args=args, kwargs=kwargs)

    def update(self, request, *args, **kwargs):
        """
        put object
        """
        return self.put_object(request, args, kwargs)

    def destroy(self, request, *args, **kwargs):
        """
        delete object
        """
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

    def put_object(self, request, args, kwargs):
        bucket_name = self.get_bucket_name(request)
        obj_path_name = self.get_obj_path_name(request)

        # 访问权限
        acl_choices = {'private': BucketFileBase.SHARE_ACCESS_NO, 'public-read': BucketFileBase.SHARE_ACCESS_READONLY,
                       'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE}
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()
        if x_amz_acl not in acl_choices:
            e = exceptions.S3InvalidRequest(f'The value {x_amz_acl} of header "x-amz-acl" is not supported.')
            return self.exception_response(request, e)

        h_manager = HarborManager()
        try:
            bucket, obj, created = h_manager.create_empty_obj(bucket_name=bucket_name, obj_path=obj_path_name,
                                                              user=request.user)
        except exceptions.S3Error as e:
            return self.exception_response(request, e)

        if x_amz_acl != 'private':
            share_code = acl_choices[x_amz_acl]
            obj.set_shared(share=share_code)

        pool_name = bucket.get_pool_name()
        obj_key = obj.get_obj_key(bucket.id)

        rados = HarborObject(pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
        if created is False:  # 对象已存在，不是新建的
            try:
                h_manager._pre_reset_upload(obj=obj, rados=rados)  # 重置对象大小
            except Exception as exc:
                return self.exception_response(request, exceptions.S3InvalidRequest(f'reset object error, {str(exc)}'))

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
                return self.exception_response(request, exceptions.S3InvalidDigest())

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

        return Response(status=status.HTTP_204_NO_CONTENT, headers={'x-amz-delete-marker': 'true'})

    def get_parsers(self):
        """
        动态分配请求体解析器
        """
        method = self.request.method.lower()
        if method == 'put':                     # put_object
            return [FileUploadParser()]

        return super().get_parsers()
