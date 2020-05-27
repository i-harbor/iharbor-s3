from django.utils.translation import gettext as _
from rest_framework.response import Response
from rest_framework import status
from rest_framework.serializers import ValidationError

from buckets.models import Bucket
from .renders import CusXMLRenderer
from .viewsets import CustomGenericViewSet
from .validators import DNSStringValidator, bucket_limit_validator
from .utils import (get_ceph_poolname_rand, BucketFileManagement, create_table_for_model_class,
                    delete_table_for_model_class)
from . import exceptions


class BucketViewSet(CustomGenericViewSet):
    renderer_classes = [CusXMLRenderer]

    def list(self, request, *args, **kwargs):
        """
        list objects (v1 && v2)
        """
        return Response(data={'msg': 'list objects'}, status=status.HTTP_400_BAD_REQUEST)

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
            self.set_renderer(request, CusXMLRenderer(root_tag_name='Error'))  # xml渲染器
            e = exceptions.S3InvalidRequest('Invalid request domain name')
            return Response(data=e.err_data(), status=e.status_code)

        return self.create_bucket(request, bucket_name)

    def destroy(self, request, *args, **kwargs):
        """
        delete bucket
        """
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
            return Response(status=status.HTTP_400_BAD_REQUEST)

        try:
            bucket_name = self.validate_create_bucket(request, bucket_name)
        except exceptions.S3Error as e:
            self.set_renderer(request, CusXMLRenderer(root_tag_name='Error'))       # xml渲染器
            return Response(data=e.err_data(), status=e.status_code)

        user = request.user
        perms = acl_choices[acl]
        pool_name = get_ceph_poolname_rand()
        bucket = Bucket(pool_name=pool_name, user=user, name=bucket_name, access_permission=perms)
        try:
            bucket.save()
        except Exception as e:
            self.set_renderer(request, CusXMLRenderer(root_tag_name='Error'))  # xml渲染器
            e = exceptions.S3InternalError(message=_('创建存储桶失败，存储桶元数据错误'))
            return Response(data=e.err_data(), status=e.status_code)

        col_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=col_name)
        model_class = bfm.get_obj_model_class()
        if not create_table_for_model_class(model=model_class):
            if not create_table_for_model_class(model=model_class):
                bucket.delete()
                delete_table_for_model_class(model=model_class)
                self.set_renderer(request, CusXMLRenderer(root_tag_name='Error'))  # xml渲染器
                e = exceptions.S3InternalError(message=_('创建存储桶失败，存储桶表错误'))
                return Response(data=e.err_data(), status=e.status_code)

        return Response(status=status.HTTP_200_OK)


class ObjViewSet(CustomGenericViewSet):
    renderer_classes = [CusXMLRenderer]

    def list(self, request, *args, **kwargs):
        bucket_name = self.get_bucket_name(request)
        obj_path_name = self.get_obj_path_name(request)
        return Response(data={'bucket_name': bucket_name, 'obj_path_name': obj_path_name}, status=status.HTTP_200_OK)
