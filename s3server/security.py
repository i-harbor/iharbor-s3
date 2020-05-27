from .settings import CEPH_RADOS

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'tbf1k*ax#48#^_qzr-c&07&z9&+8j68=x41w5lzv^wsv7=ax=v'

DEBUG = True

# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',   # 数据库引擎
        'NAME': 'webdata',       # 数据的库名，事先要创建之
        'HOST': '159.226.91.140',    # 主机
        'PORT': '3306',         # 数据库使用的端口
        'USER': 'root',  # 数据库用户名
        'PASSWORD': 'cnic.cn',  # 密码
        'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
    'metadata': {
        'ENGINE': 'django.db.backends.mysql',  # 数据库引擎
        'HOST': '159.226.91.140',  # 主机
        'PORT': '3306',  # 数据库使用的端口
        'NAME': 'metadata',  # 数据的库名，事先要创建之
        'USER': 'root',  # 数据库用户名
        'PASSWORD': 'cnic.cn',  # 密码
        'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
}

# Ceph rados settings
CEPH_RADOS['POOL_NAME'] = ('obs_test',)

# 邮箱配置
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_USE_TLS = True    #是否使用TLS安全传输协议
# EMAIL_PORT = 25
EMAIL_HOST = 'mail.cnic.cn'
EMAIL_HOST_USER = 'helpdesk@cnic.cn'
EMAIL_HOST_PASSWORD = '04D5486745ca1b18036ec2Ea6'


# 允许所有主机执行跨站点请求
CORS_ORIGIN_ALLOW_ALL = True

