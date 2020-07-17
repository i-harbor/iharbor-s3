import logging
from datetime import datetime, timedelta

from django.apps import apps
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned

from utils.md5 import to_b64, from_b64, get_str_hexMD5
from s3api.models import ObjectPartBase, MultipartUpload
from . import exceptions


logger = logging.getLogger('django.request')    # 这里的日志记录器要和setting中的loggers选项对应，不能随意给参


def get_parts_model_class(table_name):
    """
    动态创建存储桶对应的对象part模型类

    RuntimeWarning: Model 'xxxxx_' was already registered. Reloading models is not advised as it can
    lead to inconsistencies most notably with related models.
    如上述警告所述, Django 不建议重复创建Model 的定义.可以直接通过get_obj_model_class创建，无视警告.
    这里先通过 get_registered_model 获取已经注册的 Model, 如果获取不到， 再生成新的模型类.

    :param table_name: 数据库表名，模型类对应的数据库表名
    :return: Model class
    """
    model_name = 'PartsModel' + table_name
    app_leble = ObjectPartBase.Meta.app_label
    try:
        cls = apps.get_registered_model(app_label=app_leble, model_name=model_name)
        return cls
    except LookupError:
        pass

    meta = ObjectPartBase.Meta
    meta.abstract = False
    meta.db_table = table_name  # 数据库表名
    return type(model_name, (ObjectPartBase,), {'Meta': meta, '__module__': ObjectPartBase.__module__})


def create_multipart_upload_task(bucket, obj_key: str, obj_perms_code: int, obj_id: int = 0, expire_time=None):
    """
    创建一个多部分上传记录

    :param bucket: 桶实例
    :param obj_key: S3 Key, 对象全路径
    :param obj_id: 对象元数据id, 默认0不记录id
    :param obj_perms_code: 对象分享访问权限码
    :param expire_time: datetime(), 上传缓存过期时间，默认30天
    :return:
        MultipartUpload()

    :raises: S3Error
    """
    if expire_time is None:
        expire_time = datetime.now() + timedelta(days=30)  # 默认一个月到期

    try:
        upload = MultipartUpload(bucket_id=bucket.id, bucket_name=bucket.name, obj_id=obj_id,
                                 obj_key=obj_key, expire_time=expire_time, obj_perms_code=obj_perms_code)
        upload.save()
    except Exception as e:
        raise exceptions.S3InternalError(extend_msg='database error, create multipart upload.')

    return upload


def update_upload_belong_to_object(upload, bucket, obj_key: str, obj_perms_code: int, obj_id: int = 0, expire_time=None):
    """
    更新一个多部分上传记录,使记录属于指定对象

    :param upload: 多部分上传任务实例
    :param bucket: 桶实例
    :param obj_key: S3 Key, 对象全路径
    :param obj_id: 对象元数据id, 默认0不记录id
    :param obj_perms_code: 对象分享访问权限码
    :param expire_time: datetime(), 上传缓存过期时间，默认30天
    :return:
        True

    :raises: S3Error
    """
    self = upload
    need_update = ['expire_time']
    if expire_time is not None:
        self.expire_time = expire_time
    else:
        self.expire_time = datetime.now() + timedelta(days=30)      # 默认一个月到期

    if self.bucket_name != bucket.name:
        self.bucket_name = bucket.name
        need_update.append('bucket_name')

    if self.bucket_id != bucket.id:
        self.bucket_id = bucket.id
        need_update.append('bucket_id')

    if self.obj_id != obj_id:
        self.obj_id = obj_id
        need_update.append('obj_id')

    if self.obj_key != obj_key:
        self.obj_key = obj_key
        self.reset_key_md5()
        need_update.append('obj_key')
        need_update.append('key_md5')

    if self.obj_perms_code != obj_perms_code:
        self.obj_perms_code = obj_perms_code
        need_update.append('obj_perms_code')

    try:
        self.save(update_fields=need_update)
    except Exception as e:
        raise exceptions.S3InternalError(extend_msg='database error, update multipart upload.')

    return True


class MultipartUploadManager:
    def get_multipart_upload_by_id(self, upload_id: str):
        """
        查询多部分上传记录

        :param upload_id: uuid
        :return:
            MultipartUpload() or None

        :raises: S3Error
        """
        try:
            obj = MultipartUpload.objects.filter(id=upload_id).first()
        except Exception as e:
            raise exceptions.S3InternalError()

        return obj

    def get_multipart_upload_queryset(self, bucket_name: str, obj_path: str):
        """
        查询多部分上传记录

        :param bucket_name: 桶名
        :param obj_path: s3 object key
        :return:
            Queryset()

        :raises: S3Error
        """
        key_md5 = get_str_hexMD5(obj_path)
        try:
            return MultipartUpload.objects.filter(key_md5=key_md5, bucket_name=bucket_name, obj_key=obj_path).all()
        except Exception as e:
            raise exceptions.S3InternalError(extend_msg=str(e))

    def get_multipart_upload_delete_invalid(self, bucket, obj_path: str):
        """
        获取上传记录，顺便删除无效的上传记录

        :param bucket:
        :param obj_path:
        :return:
            MultipartUpload() or None

        :raises: S3Error
        """
        qs = self.get_multipart_upload_queryset(bucket_name=bucket.name, obj_path=obj_path)
        valid_uploads = []
        try:
            for upload in qs:
                if not upload.belong_to_bucket(bucket):
                    try:
                        upload.delete()
                    except Exception as e:
                        pass
                else:
                    valid_uploads.append(upload)
        except Exception as e:
            raise exceptions.S3InternalError(extend_msg='select multipart upload error.')

        if len(valid_uploads) == 0:
            return None

        return valid_uploads[0]

    @staticmethod
    def create_multipart_upload_task(bucket, obj_key: str, obj_perms_code: int, obj_id: int = 0, expire_time=None):
        return create_multipart_upload_task(bucket=bucket, obj_key=obj_key, obj_perms_code=obj_perms_code,
                                            obj_id=obj_id, expire_time=expire_time)

    @staticmethod
    def update_upload_belong_to_object(upload, bucket, obj_key: str, obj_perms_code: int, obj_id: int = 0, expire_time=None):
        return update_upload_belong_to_object(upload=upload, bucket=bucket, obj_key=obj_key, obj_perms_code=obj_perms_code,
                                              obj_id=obj_id, expire_time=expire_time)


class ObjectPartManager:
    """
    多部分上传对象管理器
    """
    def __init__(self, parts_table_name: str = '', bucket=None):
        """
        parts_table_name或bucket必须有效
        """
        self.__parts_table_name = parts_table_name
        self.bucket = bucket
        self._parts_model_class = self.creat_parts_model_class()

    def creat_parts_model_class(self):
        """
        动态创建各存储桶数据库表对应的对象part模型类
        """
        db_table = self.parts_table_name()       # 数据库表名
        return get_parts_model_class(db_table)

    def get_parts_model_class(self):
        if not self._parts_model_class:
            self._parts_model_class = self.creat_parts_model_class()

        return self._parts_model_class

    def parts_table_name(self):
        if not self.__parts_table_name:
            self.__parts_table_name = self.bucket.get_parts_table_name()

        return self.__parts_table_name

    def get_part_by_obj_id_part_num(self, obj_id: int, part_num: int):
        """
        获取对象part实例

        :param obj_id: 对象id
        :param part_num: part编号
        :return:
            obj     # success
            None    # 不存在

        :raises: Exception
        """
        model = self.get_parts_model_class()
        try:
            part = model.objects.get(obj_id=obj_id, part_num=part_num)
        except model.DoesNotExist as e:
            return None
        except MultipleObjectsReturned as e:
            msg = f'数据库表{self.parts_table_name()}中存在多个相同的part：obj_id={obj_id}, part_num={part_num}'
            logger.error(msg)
            raise exceptions.S3InternalError(message=msg)
        except Exception as e:
            msg = f'select {self.parts_table_name()},obj_id={obj_id}, part_num={part_num},err={str(e)}'
            logger.error(msg)
            raise exceptions.S3InternalError(message=msg)

        return part

    def get_part_by_upload_id_part_num(self, upload_id: str, part_num: int):
        """
        获取对象part实例

        :param upload_id: 对部分上传id
        :param part_num: part编号
        :return:
            obj     # success
            None    # 不存在

        :raises: Exception
        """
        model = self.get_parts_model_class()
        try:
            part = model.objects.get(upload_id=upload_id, part_num=part_num)
        except model.DoesNotExist as e:
            return None
        except Exception as e:
            msg = f'select {self.parts_table_name()},upload_id={upload_id}, part_num={part_num},err={str(e)}'
            logger.error(msg)
            raise exceptions.S3InternalError(message=msg)

        return part

    def get_parts_queryset_by_obj_id(self, obj_id: int):
        model = self.get_parts_model_class()
        return model.objects.filter(obj_id=obj_id).all()

    def create_part_metadata(self, upload_id: str, obj_id: int, part_num: int, size: int, part_md5: str, **kwargs):
        """
        创建一个对象部分元数据
        :return:
            part instance

        :raises: S3Error
        """
        model_class = self.get_parts_model_class()
        try:
            part = model_class(upload_id=upload_id, obj_id=obj_id, part_num=part_num,
                               size=size, part_md5=part_md5, **kwargs)
            part.save()
        except Exception as e:
            raise exceptions.S3InternalError(message='Failed to create part metadata.')

        return part

    def get_parts_queryset_by_upload_id(self, upload_id: str):
        model = self.get_parts_model_class()
        return model.objects.filter(upload_id=upload_id).all()

    def get_parts_queryset_by_upload_id_obj_id(self, upload_id: str, obj_id: int):
        model = self.get_parts_model_class()
        return model.objects.filter(upload_id=upload_id, obj_id=obj_id).all()

    def remove_object_parts(self, obj_id: int):
        """
        删除对象的part元数据
        :param obj_id: 对象id
        :return:
            True
            False
        """
        model = self.get_parts_model_class()
        try:
            r = model.objects.filter(obj_id=obj_id).delete()
        except Exception as e:
            return False

        return True



