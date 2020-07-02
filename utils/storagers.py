import hashlib

from django.conf import settings
from django.core.files.uploadhandler import FileUploadHandler
from django.core.files.uploadedfile import UploadedFile
from django.core.exceptions import RequestDataTooBig
from django.utils.translation import gettext

from utils.oss.pyrados import HarborObject, FileWrapper, RadosError
from utils.md5 import FileMD5Handler


class ParseDecodeBase64Error(Exception):
    default_status_code = 400
    default_code = "DecodeBase64Error"
    default_message = "Could not decode base64 data."


class PathParser:
    """
    路径字符串解析
    """
    def __init__(self, filepath, *args, **kwargs):
        self._path = filepath if isinstance(filepath, str) else ''  # 绝对路径， type: str

    def get_path_and_filename(self):
        """
        分割一个绝对路径，获取文件名和父路径,优先获取文件名
        :return: Tuple(path, filename)
        """
        fullpath = self._path  #.strip('/')
        if not fullpath:
            return '', ''
        l = fullpath.rsplit('/', maxsplit=1)
        filename = l[-1]
        path = l[0] if len(l) == 2 else ''
        return path, filename

    def get_bucket_path_and_filename(self):
        """
       分割一个绝对路径，获取文件名、存储通名和父路径，优先获取文件名、存储通名
       :return: Tuple(bucket_name, path, filename)
       """
        bucket_path, filename = self.get_path_and_filename()
        if not bucket_path:
            return '', '', filename
        l = bucket_path.split('/', maxsplit=1)
        bucket_name = l[0]
        path = l[-1] if len(l) == 2 else ''
        return bucket_name, path, filename

    def get_bucket_and_dirpath(self):
        """
       分割一个绝对路径，获取存储通名、文件夹路径，优先获取存储桶路径
       :return: Tuple(bucket_name, dirpath)
       """
        fullpath = self._path.strip('/')
        if not fullpath:
            return '', ''

        l = fullpath.split('/', maxsplit=1)
        bucket_name = l[0]
        dirpath = l[-1] if len(l) == 2 else ''
        return bucket_name, dirpath

    def get_bucket_path_and_dirname(self):
        """
       分割一个绝对路径，获取存储通名、文件夹名、和父路径，优先获取存储通名、文件夹名
       :return: Tuple(bucket_name, path, dirname)
       """
        bucket_name, dirpath = self.get_bucket_and_dirpath()

        if not dirpath:
            return bucket_name, '', ''

        l = dirpath.rsplit('/', maxsplit=1)
        dirname = l[-1]
        path = l[0] if len(l) == 2 else ''

        return bucket_name, path, dirname

    def get_path_breadcrumb(self, path=None):
        """
        路径面包屑
        :return: list([dir_name，dir_full_path])
        """
        breadcrumb = []
        _path = path if path is not None else self._path
        if _path == '':
            return breadcrumb

        _path = _path.strip('/')
        dirs = _path.split('/')
        for i, key in enumerate(dirs):
            breadcrumb.append([key, '/'.join(dirs[0:i+1])])
        return breadcrumb


class CephUploadFile(UploadedFile):
    """
    上传存储到ceph的一个文件
    """
    DEFAULT_CHUNK_SIZE = 5 * 2**20     # default 5MB

    def __init__(self, file, field_name, name, content_type, size, charset, file_md5='', md5_handler=None, content_type_extra=None):
        super().__init__(file, name, content_type, size, charset, content_type_extra)
        self.field_name = field_name
        self.file_md5 = file_md5
        self.md5_handler = md5_handler

    def open(self, mode=None):
        self.file.seek(0)
        return self


class FileUploadToCephHandler(FileUploadHandler):
    """
    直接存储到ceph的自定义文件上传处理器
    """
    chunk_size = 5 * 2 ** 20    # 5MB

    def __init__(self, request=None, pool_name='', obj_key=''):
        super().__init__(request=request)
        self.pool_name = pool_name
        self.obj_key = obj_key
        self.file = None
        self.file_md5_handler = None

    def handle_raw_input(self, input_data, META, content_length, boundary, encoding=None):
        """
        Handle the raw input from the client.
        """
        max_size = getattr(settings, 'CUSTOM_UPLOAD_MAX_FILE_SIZE', 5 * 2 ** 30)    # default 5GB
        if max_size is None:
            return
        if content_length > max_size:
            raise RequestDataTooBig(gettext('上传文件超过大小限制'))
        if content_length <= 0:
            raise

    def new_file(self, *args, **kwargs):
        """
        Create the file object to append to as data is coming in.
        """
        super().new_file(*args, **kwargs)
        self.file = FileWrapper(HarborObject(pool_name=self.pool_name, obj_id=self.obj_key))
        self.file_md5_handler = FileMD5Handler()

    def receive_data_chunk(self, raw_data, start):
        """
        :raises: RadosError
        """
        self.file.write(raw_data, offset=start)
        if self.file_md5_handler:
            self.file_md5_handler.update(offset=start, data=raw_data)

    def file_complete(self, file_size):
        self.file.seek(0)
        self.file.size = file_size
        return CephUploadFile(
            file=self.file,
            field_name=self.field_name,
            name=self.file_name,
            content_type=self.content_type,
            size=file_size,
            charset=self.charset,
            file_md5=self.file_md5(),
            md5_handler=self.file_md5_handler,
            content_type_extra=self.content_type_extra
        )

    def file_md5(self):
        fmh = self.file_md5_handler
        if fmh:
            return fmh.hex_md5

        return ''

