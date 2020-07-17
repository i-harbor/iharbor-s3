import uuid

from django.db import models

from utils.md5 import get_str_hexMD5


def uuid1_uuid4_hex_string():
    return uuid.uuid1().hex + uuid.uuid4().hex


class MultipartUpload(models.Model):
    """
    一个多部分上传任务
    """
    STATUS_UPLOADING = 1
    STATUS_COMPOSING = 2
    STATUS_COMPLETED = 3
    STATUS_CHOICES = (
        ('上传中', STATUS_UPLOADING),
        ('组合中', STATUS_COMPOSING),
        ('上传完成', STATUS_COMPLETED)
    )

    id = models.CharField(verbose_name='ID', primary_key=True, max_length=64, help_text='uuid1+uuid4')
    bucket_id = models.BigIntegerField(verbose_name='bucket id')
    bucket_name = models.CharField(verbose_name='bucket name', max_length=63, default='')
    obj_id = models.BigIntegerField(verbose_name='object id', default=0, help_text='组合对象后为对象id, 默认为0表示还未组合对象')
    obj_key = models.CharField(verbose_name='object key', max_length=1024, default='')
    key_md5 = models.CharField(max_length=32, verbose_name='object key MD5')
    create_time = models.DateTimeField(verbose_name='创建时间', auto_now_add=True)
    expire_time = models.DateTimeField(verbose_name='过期时间', null=True, default=None, help_text='上传过程终止时间')
    status = models.SmallIntegerField(verbose_name='状态', choices=STATUS_CHOICES, default=STATUS_UPLOADING)
    obj_perms_code = models.SmallIntegerField(verbose_name='对象访问权限', default=0)

    class Meta:
        managed = False
        db_table = 'multipart_upload'
        indexes = [
            models.Index(fields=('key_md5',), name='key_md5_idx'),
            models.Index(fields=('bucket_name',), name='bucket_name_idx')
        ]
        app_label = 'part_metadata'  # 用于db路由指定此模型对应的数据库
        verbose_name = '对象多部分上传'
        verbose_name_plural = verbose_name

    def reset_key_md5(self):
        """
        na更改时，计算并重设新的key_md5

        :return: str

        :备注：不会自动更新的数据库
        """
        key = self.obj_key if self.obj_key else ''
        self.key_md5 = get_str_hexMD5(key)

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        if not self.id:
            self.id = uuid1_uuid4_hex_string()
            if update_fields:
                update_fields.append('id')

        old_key_md5 = self.key_md5
        self.reset_key_md5()                # 每次更新，都确保key_md5和obj_key同步变更
        if self.key_md5 != old_key_md5:
            if update_fields and ('key_md5' not in update_fields):
                update_fields.append('key_md5')

        super().save(force_insert=force_insert, force_update=force_update, using=using, update_fields=update_fields)

    def belong_to_bucket(self, bucket):
        """
        此多部分上传是否属于bucket, 因为这条记录可能属于已删除的桶名相同的桶

        :param bucket: Bucket()
        :return:
            True        # 属于
            False       # 不属于桶， 无效的多部分上传记录，需删除
        """
        if (self.bucket_name == bucket.name) and (self.bucket_id == bucket.id):
            return True

        return False

    def belong_to_object(self, bucket, obj):
        """
        此多部分上传是否属于对象

        :param bucket: Bucket()
        :param obj: 对象元数据实例
        :return:
            True        # 属于
            False       # 不属于对象
        """
        if (self.belong_to_bucket(bucket)) and (self.obj_key == obj.na):
            return True

        return False

    def update_expires_time(self, time):
        """
        更新过期时间

        :param time: datetime or None
        :return:
            True
            False
        """
        self.expire_time = time
        try:
            self.save(update_fields=['expire_time'])
        except Exception as e:
            return False

        return True

    def is_composing(self):
        """
        是否正在组合对象
        :return:
            True        # 正在多部分组合对象，组合过程中，部分上传、终止部分上传，创建部分上传等操作不允许
            False
        """
        return self.status == self.STATUS_COMPOSING

    def is_uploading(self):
        """
        是否正在部分上传中
        :return:
            True        # 正在多部分上传中
            False
        """
        return self.status == self.STATUS_UPLOADING

    def is_completed(self):
        """
        是否是已完成的多部分上传任务
        :return:
            True
            False
        """
        return self.status == self.STATUS_COMPLETED

    def set_composing(self):
        """
        设置为正在组合对象
        :return:
            True or False
        """
        if self.status != self.STATUS_COMPOSING:
            self.status = self.STATUS_COMPOSING
            try:
                self.save(update_fields=['status'])
            except Exception as e:
                return False

        return True

    def set_completed(self):
        """
        对象多部分上传完成
        :return:
            True or False
        """
        if self.status != self.STATUS_COMPLETED:
            self.status = self.STATUS_COMPLETED
            try:
                self.save(update_fields=['status'])
            except Exception as e:
                return False

        return True

    def set_uploading(self):
        """
        正在多部分上传
        :return:
            True or False
        """
        if self.status != self.STATUS_UPLOADING:
            self.status = self.STATUS_UPLOADING
            try:
                self.save(update_fields=['status'])
            except Exception as e:
                return False

        return True

    def safe_delete(self):
        """
        :return:
            True
            False
        """
        try:
            self.delete()
        except Exception as e:
            return False

        return True


def build_part_rados_key(upload_id: str, part_num: int):
    return f'part_{upload_id}_{part_num}'


class ObjectPartBase(models.Model):
    """
    对象多部份上传模型基类
    """
    id = models.BigAutoField(verbose_name='ID', primary_key=True)
    upload_id = models.CharField(verbose_name='Upload ID', max_length=64, help_text='uuid')
    obj_id = models.BigIntegerField(verbose_name='所属对象ID', default=0, help_text='组合对象后为对象id, 默认为0表示还未组合对象')
    part_num = models.IntegerField(verbose_name='编号')
    size = models.BigIntegerField(verbose_name='块大小', default=0)
    obj_offset = models.BigIntegerField(verbose_name='对象偏移量', default=-1, help_text='块在对象中的偏移量')
    part_md5 = models.CharField(verbose_name='MD5', max_length=32, default='')
    modified_time = models.DateTimeField(verbose_name='修改时间', auto_now=True)
    obj_etag = models.CharField(verbose_name='ETag', max_length=64, default='')

    class Meta:
        unique_together = ['upload_id', 'part_num']
        indexes = [models.Index(fields=('obj_id',), name='obj_id_idx')]
        abstract = True
        app_label = 'part_metadata'  # 用于db路由指定此模型对应的数据库
        verbose_name = '对象part抽象基类'
        verbose_name_plural = verbose_name

    def __repr__(self):
        return f'ObjectPart(obj_id={self.obj_id}, part_num={self.part_num})'

    def __str__(self):
        return self.__repr__()

    def get_part_rados_key(self):
        return build_part_rados_key(upload_id=self.upload_id, part_num=self.part_num)

    def safe_delete(self, using=None, keep_parents=False):
        try:
            self.delete(using=using, keep_parents=keep_parents)
        except Exception as e:
            return False

        return True
