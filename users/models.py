import os
import binascii
from uuid import uuid1

from django.db import models
from django.contrib.auth.models import AbstractUser
from django.utils import timezone


class UserProfile(AbstractUser):
    """
    自定义用户模型
    """
    NON_THIRD_APP = 0
    LOCAL_USER = NON_THIRD_APP
    THIRD_APP_KJY = 1  # 第三方科技云通行证

    THIRD_APP_CHOICES = (
        (NON_THIRD_APP, 'Local user.'),
        (THIRD_APP_KJY, '科技云通行证')
    )
    ROLE_NORMAL = 0
    ROLE_SUPPER_USER = 1
    ROLE_APP_SUPPER_USER = 2  # 第三方APP超级用户,有权限获取普通用户安全凭证
    ROLE_STAFF = 4
    ROLE_CHOICES = (
        (ROLE_NORMAL, '普通用户'),
        (ROLE_SUPPER_USER, '超级用户'),
        (ROLE_APP_SUPPER_USER, '第三方APP超级用户')
    )

    telephone = models.CharField(verbose_name='电话', max_length=11, default='')
    company = models.CharField(verbose_name='公司/单位', max_length=255, default='')
    third_app = models.SmallIntegerField(verbose_name='第三方应用登录', choices=THIRD_APP_CHOICES, default=NON_THIRD_APP)
    secret_key = models.CharField(verbose_name='个人密钥', max_length=20, blank=True, default='')  # jwt加密解密需要
    last_active = models.DateField(verbose_name='最后活跃日期', db_index=True, default=timezone.now)
    role = models.SmallIntegerField(verbose_name='角色权限', choices=ROLE_CHOICES, default=ROLE_NORMAL)

    class Meta:
        managed = False
        ordering = ['-id']
        verbose_name = '用户'
        verbose_name_plural = '用户'


class AuthKey(models.Model):
    STATUS_CHOICES = (
        (True, '正常'),
        (False, '停用'),
    )

    READ_WRITE = 0
    READ_ONLY = 1
    READ_WRITE_CHOICES = (
        (READ_WRITE, '可读可写'),
        (READ_ONLY, '只读'),
    )

    id = models.CharField(verbose_name='access_key', max_length=50, primary_key=True)
    secret_key = models.CharField(verbose_name='secret_key', max_length=50, default='')
    user = models.ForeignKey(to=UserProfile, on_delete=models.CASCADE, verbose_name='所属用户')
    state = models.BooleanField(verbose_name='状态', default=True, choices=STATUS_CHOICES, help_text='正常或者停用')
    create_time = models.DateTimeField(verbose_name='创建时间', auto_now_add=True)
    permission = models.IntegerField(verbose_name='读写权限', default=READ_WRITE, choices=READ_WRITE_CHOICES)

    class Meta:
        managed = False
        verbose_name = '访问密钥'
        verbose_name_plural = '访问密钥'
        ordering = ['-create_time']

    def _get_access_key_val(self):
        return self.id

    def _set_access_key_val(self, value):
        self.id = value

    access_key = property(_get_access_key_val, _set_access_key_val)

    def save(self, *args, **kwargs):
        # access_key
        if not self.id:
            self.id = self.uuid1_hex_key()

        if not self.secret_key:
            self.secret_key = self.generate_key()
        return super(AuthKey, self).save(*args, **kwargs)

    def generate_key(self):
        """
        生成一个随机字串
        """
        return binascii.hexlify(os.urandom(20)).decode()

    def uuid1_hex_key(self):
        """
        唯一uuid1
        """
        return uuid1().hex

    def is_key_active(self):
        """
        密钥是否是激活有效的

        :return:
            有效：True
            停用：False
        """
        return self.state

    def __str__(self):
        return self.secret_key
