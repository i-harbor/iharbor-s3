import random
import logging
import traceback
import os

from django.db.backends.mysql.schema import DatabaseSchemaEditor
from django.db import connections, router
from django.db.models import Sum, Count
from django.db.models.query import Q
from django.db.utils import ProgrammingError
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.apps import apps
from django.conf import settings

from buckets.models import BucketFileBase, get_str_hexMD5
from utils.oss.pyrados import HarborObject, ObjectPart, RadosError


logger = logging.getLogger('django.request')


class InvalidPathError(Exception):
    pass


def build_harbor_object(using: str, pool_name: str, obj_id: str, obj_size: int = 0):
    """
    构建iharbor对象对应的ceph读写接口

    :param using: ceph集群配置别名，对应对象数据所在ceph集群
    :param pool_name: ceph存储池名称，对应对象数据所在存储池名称
    :param obj_id: 对象在ceph存储池中对应的rados名称
    :param obj_size: 对象的大小
    """
    cephs = settings.CEPH_RADOS
    if using not in cephs:
        raise RadosError(f'别名为"{using}"的CEPH集群信息未配置，请确认配置文件中的“CEPH_RADOS”配置内容')

    ceph = cephs[using]
    cluster_name = ceph['CLUSTER_NAME']
    user_name = ceph['USER_NAME']
    conf_file = ceph['CONF_FILE_PATH']
    keyring_file = ceph['KEYRING_FILE_PATH']
    return HarborObject(pool_name=pool_name, obj_id=obj_id, obj_size=obj_size, cluster_name=cluster_name,
                        user_name=user_name, conf_file=conf_file, keyring_file=keyring_file)


def build_harbor_object_part(using: str, part_key: str, part_size: int = 0, pool_name: str = None):
    """
    构建iharbor对象多部份上传part对应的ceph读写接口

    :param using: ceph集群配置别名，对应对象数据所在ceph集群
    :param pool_name: ceph存储池名称，对应对象数据所在存储池名称
    :param part_key: 对象part在ceph存储池中对应的rados名称
    :param part_size: 对象的part大小
    """
    cephs = settings.CEPH_RADOS
    if using not in cephs:
        raise RadosError(f'别名为"{using}"的CEPH集群信息未配置，请确认配置文件中的“CEPH_RADOS”配置内容')

    ceph = cephs[using]
    cluster_name = ceph['CLUSTER_NAME']
    user_name = ceph['USER_NAME']
    conf_file = ceph['CONF_FILE_PATH']
    keyring_file = ceph['KEYRING_FILE_PATH']
    if not pool_name:
        pool_name = ceph['MULTIPART_POOL_NAME']
    return ObjectPart(pool_name=pool_name, part_key=part_key, part_size=part_size, cluster_name=cluster_name,
                        user_name=user_name, conf_file=conf_file, keyring_file=keyring_file)


def check_ceph_settins():
    def raise_msg(msg):
        print(msg)
        raise Exception(msg)

    cephs = getattr(settings, 'CEPH_RADOS', None)
    if not cephs:
        raise_msg('未配置CEPH集群信息，配置文件中配置“CEPH_RADOS”')

    if 'default' not in cephs:
        raise_msg('配置文件中CEPH集群信息配置“CEPH_RADOS”中必须存在一个别名“default”')

    enable_choices = []
    for using in cephs:
        if len(using) >= 16:
            raise_msg(f'CEPH集群配置“CEPH_RADOS”中，别名"{using}"太长，不能超过16字符')

        ceph = cephs[using]
        conf_file = ceph['CONF_FILE_PATH']
        if not os.path.exists(conf_file):
            raise_msg(f'别名为“{using}”的CEPH集群配置文件“{conf_file}”不存在')

        keyring_file = ceph['KEYRING_FILE_PATH']
        if not os.path.exists(keyring_file):
            raise_msg(f'别名为“{using}”的CEPH集群keyring配置文件“{keyring_file}”不存在')

        if 'USER_NAME' not in ceph:
            raise_msg(f'别名为“{using}”的CEPH集群配置信息未设置“USER_NAME”')

        if 'POOL_NAME' not in ceph:
            raise_msg(f'别名为“{using}”的CEPH集群配置信息未设置“POOL_NAME”')

        if not (isinstance(ceph['POOL_NAME'], str) or isinstance(ceph['POOL_NAME'], tuple)):
            raise_msg(f'别名为“{using}”的CEPH集群配置信息“POOL_NAME”必须是str或者tuple')

        if 'MULTIPART_POOL_NAME' not in ceph:
            raise_msg(f'别名为“{using}”的CEPH集群配置信息未设置“MULTIPART_POOL_NAME”')

        ho = build_harbor_object(using=using, pool_name='', obj_id='')
        try:
            with ho.rados:
                pass
        except Exception as e:
            raise_msg(f'别名为“{using}”的CEPH集群连接错误，{str(e)}')

        if ('DISABLE_CHOICE' in ceph) and (ceph['DISABLE_CHOICE'] is True):
            continue

        enable_choices.append(using)

    if not enable_choices:
        raise_msg('没有可供选择的CEPH集群配置，创建bucket时没有可供选择的CEPH集群，'
                  '请至少确保有一个CEPH集群配置“DISABLE_CHOICE”为False')


def get_ceph_alias_rand():
    """
    从配置的CEPH集群中随机获取一个ceph集群的配置的别名
    :return:
        str

    :raises: ValueError
    """
    cephs = settings.CEPH_RADOS
    aliases = []
    for k in cephs.keys():
        ceph = cephs[k]
        if ('DISABLE_CHOICE' in ceph) and (ceph['DISABLE_CHOICE'] is True):
            continue

        aliases.append(k)

    if not aliases:
        raise ValueError('配置文件CEPH_RADOS中没有可供选择的CEPH集群配置')

    return random.choice(aliases)


def get_ceph_poolname_rand(using: str):
    """
    从配置的CEPH pool name随机获取一个
    :return:
        poolname: str

    :raises: ValueError
    """
    pools = settings.CEPH_RADOS[using].get('POOL_NAME', None)
    if not pools:
        raise ValueError(f'配置文件CEPH_RADOS中别名“{using}”配置中POOL_NAME配置项无效')

    if isinstance(pools, str):
        return pools

    if isinstance(pools, tuple) or isinstance(pools, list):
        return random.choice(pools)

    raise ValueError(f'配置文件CEPH_RADOS中别名“{using}”配置中POOL_NAME配置项需要是一个元组tuple')


def create_table_for_model_class(model, ):
    """
    创建Model类对应的数据库表

    :param model: Model类
    :return:
            True: success
            False: failure
    """
    try:
        using = router.db_for_write(model)
        with DatabaseSchemaEditor(connection=connections[using]) as schema_editor:
            schema_editor.create_model(model)
            if issubclass(model, BucketFileBase):
                try:
                    table_name = schema_editor.quote_name(model._meta.db_table)
                    sql1 = f"ALTER TABLE {table_name} CHANGE COLUMN `na` `na` LONGTEXT NOT " \
                           f"NULL COLLATE 'utf8_bin' AFTER `id`;"
                    sql2 = f"ALTER TABLE {table_name} CHANGE COLUMN `name` `name` VARCHAR(255) " \
                           f"NOT NULL COLLATE 'utf8_bin' AFTER `na_md5`;"
                    schema_editor.execute(sql=sql1)
                    schema_editor.execute(sql=sql2)
                except Exception as exc:
                    if delete_table_for_model_class(model):
                        raise exc       # model table 删除成功，抛出错误
    except Exception as e:
        msg = traceback.format_exc()
        logger.error(msg)
        return False

    return True


def delete_table_for_model_class(model):
    """
    删除Model类对应的数据库表

    :param model: Model类
    :return:
            True: success
            False: failure
    """
    try:
        using = router.db_for_write(model)
        with DatabaseSchemaEditor(connection=connections[using]) as schema_editor:
            schema_editor.delete_model(model)
    except (Exception, ProgrammingError) as e:
        msg = traceback.format_exc()
        logger.error(msg)
        if e.args[0] in [1051, 1146]:  # unknown table or table not exists
            return True

        return False

    return True


def is_model_table_exists(model):
    """
    检查模型类Model的数据库表是否已存在
    :param model:
    :return: True(existing); False(not existing)
    """
    using = router.db_for_write(model)
    connection = connections[using]
    if hasattr(model, '_meta'):
        db_table = model._meta.db_table
    else:
        db_table = model.Meta.db_table
    return db_table in connection.introspection.table_names()


def get_obj_model_class(table_name):
    """
    动态创建存储桶对应的对象模型类

    RuntimeWarning: Model 'xxxxx_' was already registered. Reloading models is not advised as it can
    lead to inconsistencies most notably with related models.
    如上述警告所述, Django 不建议重复创建Model 的定义.可以直接通过get_obj_model_class创建，无视警告.
    这里先通过 get_registered_model 获取已经注册的 Model, 如果获取不到， 再生成新的模型类.

    :param table_name: 数据库表名，模型类对应的数据库表名
    :return: Model class
    """
    model_name = 'ObjModel' + table_name
    app_leble = BucketFileBase.Meta.app_label
    try:
        cls = apps.get_registered_model(app_label=app_leble, model_name=model_name)
        return cls
    except LookupError:
        pass

    meta = BucketFileBase.Meta
    meta.abstract = False
    meta.db_table = table_name  # 数据库表名
    return type(model_name, (BucketFileBase,), {'Meta': meta, '__module__': BucketFileBase.__module__})


def get_bfmanager(path='', table_name=''):
    return BucketFileManagement(path=path, collection_name=table_name)


class BucketFileManagement:
    """
    存储桶相关的操作方法类
    """
    ROOT_DIR_ID = 0     # 根目录ID
    InvalidPathError = InvalidPathError

    def __init__(self, path='', collection_name='', *args, **kwargs):
        self._path = self._hand_path(path)
        self._collection_name = collection_name     # bucket's database table name
        self.cur_dir_id = None
        self._bucket_file_class = self.creat_obj_model_class()

    def creat_obj_model_class(self):
        """
        动态创建各存储桶数据库表对应的模型类
        """
        db_table = self.get_collection_name()   # 数据库表名
        return get_obj_model_class(db_table)

    def get_obj_model_class(self):
        if not self._bucket_file_class:
            self._bucket_file_class = self.creat_obj_model_class()

        return self._bucket_file_class

    def root_dir(self):
        """
        根目录对象
        :return:
        """
        c = self.get_obj_model_class()
        return c(id=self.ROOT_DIR_ID, na='', name='', fod=False, did=self.ROOT_DIR_ID, si=0)

    def get_collection_name(self):
        return self._collection_name

    def _hand_path(self, path):
        """ath字符串两边可能的空白和右边/"""
        if isinstance(path, str):
            path.strip(' ')
            return path.rstrip('/')
        return ''

    def get_cur_dir_id(self, dir_path=None):
        """
        获得当前目录节点id
        :return: (ok, id)，ok指示是否有错误(路径参数错误)
            正常返回：(True, id)，顶级目录时id=ROOT_DIR_ID
            未找到记录返回(False, None)，即参数有误

        :raises: Exception
        """
        if self.cur_dir_id:
            return True, self.cur_dir_id

        path = dir_path if dir_path else self._path
        # path为空，根目录为存储桶
        if path == '' or path == '/':
            return True, self.ROOT_DIR_ID

        path = self._hand_path(path)
        if not path:
            return False, None  # path参数有误

        try:
            obj = self.get_obj(path=path)
        except Exception as e:
            raise Exception(f'查询目录id错误，{str(e)}')
        if obj and obj.is_dir():
            self.cur_dir_id = obj.id
            return True, self.cur_dir_id

        return False, None  # path参数有误,未找到对应目录信息

    def get_cur_dir_files(self, cur_dir_id=None):
        """
        获得当前目录下的文件或文件夹记录

        :param cur_dir_id: 目录id;
        :return: 目录id下的文件或目录记录list; id==None时，返回存储桶下的文件或目录记录list

        :raises: Exception
        """
        dir_id = None
        if cur_dir_id is not None:
            dir_id = cur_dir_id

        if dir_id is None and self._path:
            ok, dir_id = self.get_cur_dir_id()

            # path路径有误
            if not ok:
                return False, None

        model_class = self.get_obj_model_class()
        try:
            if dir_id:
                files = model_class.objects.filter(did=dir_id).all()
            else:
                # 存储桶下文件目录,did=0表示是存储桶下的文件目录
                files = model_class.objects.filter(did=self.ROOT_DIR_ID).all()
        except Exception as e:
            logger.error('In get_cur_dir_files:' + str(e))
            return False, None

        return True, files

    def get_file_exists(self, file_name):
        """
        通过文件名获取当前目录下的文件信息

        :param file_name: 文件名
        :return: 如果存在返回文件记录对象，否则None

        :raises: Exception, InvalidPathError
        """
        file_name = file_name.strip('/')
        obj = self.get_dir_or_obj_exists(name=file_name)
        if obj and obj.is_file():
            return obj

        return None

    def get_dir_exists(self, dir_name):
        """
        通过目录名获取当前目录下的目录信息
        :param dir_name: 目录名称（不含父路径）
        :return:
            目录对象 or None
            raises: Exception   # 发生错误，或当前目录参数有误，对应目录不存在

        :raises: Exception, InvalidPathError
        """
        obj = self.get_dir_or_obj_exists(name=dir_name)
        if obj and obj.is_dir():
            return obj

        return None

    def get_dir_or_obj_exists(self, name, check_path_exists: bool = True):
        """
        通过名称获取当前路径下的子目录或对象
        :param name: 目录名或对象名称
        :param check_path_exists: 是否检查当前路径是否存在
        :return:
            文件目录对象 or None
            raises: Exception   # 发生错误，或当前目录参数有误，对应目录不存在

        :raises: Exception, InvalidPathError
        """
        if check_path_exists:
            ok, did = self.get_cur_dir_id()
            if not ok:
                raise InvalidPathError(f'父路径（{self._path}）不存在，或路径有误')

        path = self.build_dir_full_name(name)
        try:
            dir_or_obj = self.get_obj(path=path)
        except Exception as e:
            raise Exception(f'查询目录id错误，{str(e)}')

        return dir_or_obj

    def build_dir_full_name(self, dir_name):
        """
        拼接全路径

        :param dir_name: 目录名
        :return: 目录绝对路径
        """
        dir_name.strip('/')
        path = self._hand_path(self._path)
        return (path + '/' + dir_name) if path else dir_name

    def get_file_obj_by_id(self, id):
        """
        通过id获取文件对象
        :return:
        """
        model_class = self.get_obj_model_class()
        try:
            bfis = model_class.objects.get(id=id)
        except model_class.DoesNotExist:
            return None

        return bfis.first()

    def get_count(self):
        """
        获取存储桶数据库表的对象和目录记录总数量
        :return:
        """
        return self.get_obj_model_class().objects.count()

    def get_obj_count(self):
        """
        获取存储桶中的对象总数量
        :return:
        """
        return self.get_obj_model_class().objects.filter(fod=True).count()

    def get_valid_obj_count(self):
        """
        获取存储桶中的有效（未删除状态）对象数量
        :return:
        """
        return self.get_obj_model_class().objects.filter(Q(fod=True) & Q(sds=False)).count()

    def cur_dir_is_empty(self):
        """
        当前目录是否为空目录
        :return:True(空); False(非空); None(有错误或目录不存在)

        :raises: Exception
        """
        ok, did = self.get_cur_dir_id()
        # 有错误发生
        if not ok:
            return None

        # 未找到目录
        if did is None:
            return None

        if self.get_obj_model_class().objects.filter(did=did).exists():
            return False

        return True

    def dir_is_empty(self, dir_obj):
        """
        给定目录是否为空目录

        :params dir_obj: 目录对象
        :return:True(空); False(非空)
        """
        did = dir_obj.id

        if self.get_obj_model_class().objects.filter(did=did).exists():
            return False

        return True

    def get_bucket_space_and_count(self):
        """
        获取存储桶中的对象占用总空间大小和对象数量
        :return:
            {'space': 123, 'count: 456}
        """
        data = self.get_obj_model_class().objects.filter(fod=True).aggregate(space=Sum('si'), count=Count('fod'))
        return data

    def get_obj(self, path: str):
        """
        获取目录或对象

        :param path: 目录或对象路径
        :return:
            obj     # success
            None    # 不存在

        :raises: Exception
        """
        na_md5 = get_str_hexMD5(path)
        model_class = self.get_obj_model_class()
        try:
            obj = model_class.objects.get(Q(na_md5=na_md5) | Q(na_md5__isnull=True), na=path)
        except model_class.DoesNotExist as e:
            return None
        except MultipleObjectsReturned as e:
            msg = f'数据库表{self.get_collection_name()}中存在多个相同的目录：{path}'
            logger.error(msg)
            raise Exception(msg)
        except Exception as e:
            msg = f'select {self.get_collection_name()},path={path},err={str(e)}'
            logger.error(msg)
            raise Exception(msg)

        return obj

    def get_objects_dirs_queryset(self):
        """
        获得所有文件对象和目录记录

        :return: QuerySet()
        """
        model_class = self.get_obj_model_class()
        return model_class.objects.all()

    def get_prefix_objects_dirs_queryset(self, prefix: str):
        """
        获得指定路径前缀的对象和目录查询集

        :param prefix: 路径前缀
        :return: QuerySet()
        """
        model_class = self.get_obj_model_class()
        return model_class.objects.filter(na__startswith=prefix).all()

