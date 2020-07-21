from .settings import CEPH_RADOS

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'xxx'

DEBUG = False

# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',   # 数据库引擎
        'NAME': 'xxx',       # 数据的库名，事先要创建之
        'HOST': 'xxx.xxx.xxx.xxx',    # 主机
        'PORT': '3306',         # 数据库使用的端口
        'USER': 'xxx',  # 数据库用户名
        'PASSWORD': 'xxx',  # 密码
        'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
    'metadata': {
        'ENGINE': 'django.db.backends.mysql',  # 数据库引擎
        'HOST': 'xxx.xxx.xxx.xxx',  # 主机
        'PORT': '3306',  # 数据库使用的端口
        'NAME': 'xxx',  # 数据的库名，事先要创建之
        'USER': 'xxx',  # 数据库用户名
        'PASSWORD': 'xxx',  # 密码
        'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
    'part_metadata': {
        'ENGINE': 'django.db.backends.mysql',  # 数据库引擎
        'HOST': 'xxx.xxx.xxx.xxx',  # 主机
        'PORT': '3306',  # 数据库使用的端口
        'NAME': 'xxx',  # 数据的库名，事先要创建之
        'USER': 'xxx',  # 数据库用户名
        'PASSWORD': 'xxx',  # 密码
        'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
}

# Ceph rados settings
CEPH_RADOS['POOL_NAME'] = ('obs_test',)


# 允许所有主机执行跨站点请求
CORS_ORIGIN_ALLOW_ALL = True

