# if __name__ == "__main__":
#     import os
#     import sys
#     import django
#     # 将项目路径添加到系统搜寻路径当中，查找方式为从当前脚本开始，找到要调用的django项目的路径
#     sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
#     # 设置项目的配置文件 不做修改的话就是 settings 文件
#     os.environ.setdefault("DJANGO_SETTINGS_MODULE", "s3server.settings")
#     django.setup()  # 加载项目配置

from datetime import datetime, timedelta

from django.core.management.base import BaseCommand, CommandError

from s3api.models import MultipartUpload, build_part_rados_key
from s3api.managers import ObjectPartManager
from s3api.handlers import MULTIPART_UPLOAD_MAX_SIZE
from buckets.models import Bucket, Archive, build_parts_tablename
from utils.oss.pyrados import ObjectPart


class Command(BaseCommand):
    """
    清理多部分上传
    """

    help = """** manage.py clear_multipart_upload -h **"""
    days_ago = 30

    def add_arguments(self, parser):
        parser.add_argument(
            # 当命令行有此参数时取值const, 否则取值default
            '--clear-all', default=False, nargs='?', dest='clear_all', type=bool, const=True,
            help='同时清理存储桶和归档的存储桶的多部分上传',
        )
        parser.add_argument(
            '--bucket', default='', dest='bucket_name', type=str,
            help='The multipart upload belonging to this bucket will be clear.',
        )
        parser.add_argument(
            '--days-ago', default='30', dest='days-ago', type=int,
            help='Clear objects and directories that have been deleted more than days ago.',
        )

    def handle(self, *args, **options):
        days_ago = options.get('days-ago', 30)
        try:
            days_ago = int(days_ago)
            if days_ago < 0:
                days_ago = 0
        except Exception as e:
            raise CommandError(f"Clearing buckets cancelled. invalid value of '--days-ago', {str(e)}")

        self.days_ago = days_ago
        bucket_name = options['bucket_name']
        clear_all = options['clear_all']

        if input('Are you sure to create the table?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        if bucket_name:
            self.handle_by_bucket_name(bucket_name, clear_all)
        else:
            self.handle_all()

        self.stdout.write(self.style.WARNING('End'))

    def handle_all(self):
        buckets = self.get_bucket_archive()
        length = len(buckets)
        if length > 0:
            self.stdout.write(self.style.WARNING(f'开始清理{length}个归档存储桶.'))
            self.clear_buckets(buckets)

        qs = self.get_multipart_queryset(days_ago=self.days_ago)
        for uploads in self.chunk_queryset(qs, 1):
            self.clear_uploads(uploads)

    def clear_uploads(self, uploads):
        for upload in uploads:
            parts_tablename = build_parts_tablename(upload.bucket_id)
            self.clear_one_mulitipart(parts_tablename, upload)

    def handle_by_bucket_name(self, bucket_name: str, clear_all: bool):
        # 删除归档的存储桶
        buckets = self.get_bucket_archive(bucket_name)
        length = len(buckets)
        if length > 0:
            self.stdout.write(self.style.WARNING(f'开始清理{length}个归档存储桶.'))
            self.clear_buckets(buckets)
        else:
            self.stdout.write(self.style.WARNING(f'不存在归档存储桶"{bucket_name}".'))

        if clear_all:
            bucket = self.get_bucket(bucket_name)
            if bucket:
                self.stdout.write(self.style.WARNING(f'开始清理存储桶"{bucket_name}".'))
                self.clear_buckets([bucket])
            else:
                self.stdout.write(self.style.WARNING(f'不存在存储桶"{bucket_name}".'))

    def clear_buckets(self, buckets):
        for b in buckets:
            part_table_name = b.get_parts_table_name()
            qs = self.get_multipart_queryset(bucket=b, days_ago=self.days_ago)
            for upload in qs:
                self.clear_one_mulitipart(part_table_name=part_table_name, upload=upload)

    @staticmethod
    def get_multipart_queryset(bucket=None, days_ago: int = 30):
        lookups = {}
        if isinstance(bucket, Bucket):
            lookups['bucket_name'] = bucket.name
            lookups['bucket_id'] = bucket.id
        elif isinstance(bucket, Archive):
            lookups['bucket_name'] = bucket.name
            lookups['bucket_id'] = bucket.original_id

        lookups['create_time__lt'] = datetime.utcnow() - timedelta(days=days_ago)
        return MultipartUpload.objects.filter(**lookups).order_by('create_time').all()

    def clear_one_mulitipart(self, part_table_name: str, upload):
        """
        清理一个多部分上传

        :param part_table_name: part表名称
        :param upload: MultipartUpload()
        """
        upload_id = upload.id
        opm = ObjectPartManager(parts_table_name=part_table_name)
        try:
            parts_qs = opm.get_parts_queryset_by_upload_id(upload_id=upload.id)
            _ = len(parts_qs)
        except Exception as e:
            if e.args[0] == 1146:       # 数据库表不存在或已删除
                self.stdout.write(self.style.SUCCESS(f'{str(e)}, try clear rados.'))
                ok = self.try_clear_upload_part_rados(upload=upload)
                if ok:
                    if upload.safe_delete():
                        self.stdout.write(self.style.SUCCESS(f'OK deleted upload<{upload_id}>.'))
                        return True

            self.stdout.write(self.style.ERROR(f'Failed to delete upload<{upload_id}>.'))
            return False

        failed_parts = self.clear_parts_cache(parts_qs)
        if failed_parts:
            self.stdout.write(self.style.ERROR(f'Failed to delete upload<{upload_id}>.'))
        else:
            if upload.safe_delete():
                self.stdout.write(self.style.SUCCESS(f'OK deleted upload<{upload_id}>.'))
                return True
            else:
                self.stdout.write(self.style.ERROR(f'Failed to delete upload<{upload_id}>.'))
                return False

    @staticmethod
    def try_clear_upload_part_rados(upload):
        """
        尝试清除可能的多部分上传的part rados数据
        :return:
            True    # 删除成功
            False   # 删除失败
        """
        not_exist_count = 0
        part_rados = ObjectPart(part_key='', part_size=0)
        for part_num in range(1, 10001):
            part_key = build_part_rados_key(upload_id=upload.id, part_num=part_num)
            part_rados.reset_part_key_and_size(part_key=part_key, part_size=MULTIPART_UPLOAD_MAX_SIZE)
            ok, _ = part_rados.delete_or_notfound()
            if ok is False:
                ok, _ = part_rados.delete_or_notfound()

            if ok is False:
                return False
            elif ok is None:
                not_exist_count += 1
                if not_exist_count >= 10:       # 多次连续都不存在，默认多部分上传的所有part数据清理完了
                    return True
            else:
                not_exist_count = 0

    @staticmethod
    def clear_parts_cache(parts):
        """
        清理part缓存，part rados数据或元数据

        :param parts: part元数据实例list或dict
        :return:
            [part]              # 删除失败的part元数据list
        """
        remove_failed_parts = []  # 删除元数据失败的part
        part_rados = ObjectPart(part_key='', part_size=0)
        for p in parts:
            part_rados.reset_part_key_and_size(part_key=p.get_part_rados_key(), part_size=p.size)
            ok, _ = part_rados.delete()
            if not ok:
                ok, _ = part_rados.delete()  # 重试一次
                if not ok:
                    remove_failed_parts.append(p)
                    continue

            if not p.safe_delete():
                if not p.safe_delete():  # 重试一次
                    remove_failed_parts.append(p)

        return remove_failed_parts

    @staticmethod
    def get_bucket(name: str):
        return Bucket.objects.filter(name=name, type=Archive.TYPE_S3).first()

    @staticmethod
    def get_bucket_archive(name: str = None):
        """
        获取指定桶名或全部归档桶
        :param name:
        :return:
            Queryset()
        """
        if name:
            return Archive.objects.filter(name=name, type=Archive.TYPE_S3).all()

        return Archive.objects.filter(type=Archive.TYPE_S3).all()

    @staticmethod
    def chunk_queryset(qs, num_per: int = 1000):
        while True:
            r = qs.all()[:num_per]
            if len(r) > 0:
                yield r
            else:
                break


# if __name__ == "__main__":
#     Command().handle(**{'bucket_name': '', 'clear_all': True})
