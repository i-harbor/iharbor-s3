import threading
from datetime import timedelta

from django.utils import timezone
from django.core.management.base import BaseCommand, CommandError
from django.db.utils import ProgrammingError

from s3api.utils import BucketFileManagement, delete_table_for_model_class
from buckets.models import Archive
from s3api.managers import get_parts_model_class
from utils.oss.pyrados import build_harbor_object
from s3api.models import MultipartUpload


class Command(BaseCommand):
    """
    清理bucket命令，清理满足彻底删除条件的对象和目录
    """
    pool_sem = threading.Semaphore(10)  # 定义最多同时启用多少个线程

    help = 'Really delete objects and directories that have been deleted from a bucket'
    _clear_datetime = None

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucket-name',
            help='Name of bucket have been deleted will be clearing,',
        )

        parser.add_argument(
            '--days-ago', default='30', dest='days-ago', type=int,
            help='Clear objects and directories that have been deleted more than days ago.',
        )
        parser.add_argument(
            # 当命令行有此参数时取值const, 否则取值default
            '--all-deleted', default=None, nargs='?', dest='all_deleted', const=True,
            help='All buckets that have been deleted will be clearing.',
        )
        parser.add_argument(
            '--max-threads', default=10, dest='max-threads', type=int,
            help='max threads on multithreading mode.',
        )

    def handle(self, *args, **options):
        max_threads = options.get('max-threads')
        if not max_threads or max_threads < 1:
            raise CommandError(f"Clearing buckets cancelled. invalid value of 'max-threads', {max_threads}")

        self.pool_sem = threading.Semaphore(max_threads)  # 定义最多同时启用多少个线程

        days_ago = options.get('days-ago', 30)
        try:
            days_ago = int(days_ago)
            if days_ago < 0:
                days_ago = 0
        except Exception as e:
            raise CommandError(f"Clearing buckets cancelled. invalid value of '--days-ago', {str(e)}")

        self._clear_datetime = timezone.now() - timedelta(days=days_ago)

        buckets = self.get_buckets(**options)
        self.stdout.write(self.style.NOTICE(f'days-ago: {days_ago}, max threads {max_threads}'))
        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("Clearing buckets cancelled.")

        self.clear_buckets(buckets)

    def get_buckets(self, **options):
        """
        获取给定的bucket或所有bucket
        :param options:
        :return:
        """
        bucket_name = options['bucket-name']
        all_deleted = options['all_deleted']

        # 指定名字的桶
        if bucket_name:
            self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucket_name)))
            return Archive.objects.filter(name=bucket_name, type=Archive.TYPE_S3,
                                          archive_time__lt=self._clear_datetime).all()

        # 全部已删除归档的桶
        if all_deleted:
            self.stdout.write(self.style.NOTICE('Will clear all buckets that have been softly deleted '))
            return Archive.objects.filter(type=Archive.TYPE_S3, archive_time__lt=self._clear_datetime).all()

        # 未给出参数
        if not bucket_name:
            bucket_name = input('Please input a bucket name:')

        self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucket_name)))
        return Archive.objects.filter(name=bucket_name, type=Archive.TYPE_S3).all()

    def get_objs_and_dirs(self, model_class, num=100):
        """
        获取对象,默认最多返回1000条

        :param model_class: 对象和目录的模型类
        :param num: 获取数量
        :return:
        """
        try:
            objs = model_class.objects.filter(fod=True).all()[:num]
        except Exception as e:
            self.stdout.write(self.style.ERROR('Error when clearing bucket table named {0},'.format(
                model_class.Meta.db_table) + str(e)))
            return None

        return objs

    def is_meet_delete_time(self, bucket):
        """
        归档的桶是否满足删除时间要求，即是否可以清理

        :param bucket: Archive()
        :return:
            True    # 满足
            False   # 不满足
        """
        archive_time = bucket.archive_time.replace(tzinfo=None)

        if timezone.is_aware(self._clear_datetime):
            if not timezone.is_aware(archive_time):
                archive_time = timezone.make_aware(archive_time)
        else:
            if not timezone.is_naive(archive_time):
                archive_time = timezone.make_naive(archive_time)

        if archive_time < self._clear_datetime:
            return True

        return False

    def thread_clear_one_bucket(self, bucket):
        try:
            self.clear_one_bucket(bucket)
        finally:
            self.pool_sem.release()  # 可用线程数+1

    def clear_one_bucket(self, bucket):
        """
        清除一个bucket中满足删除条件的对象和目录

        :param bucket: Archive()
        :return:
        """
        self.stdout.write('Now clearing bucket named {0}'.format(bucket.name))
        table_name = bucket.get_bucket_table_name()
        model_class = BucketFileManagement(collection_name=table_name).get_obj_model_class()

        # 已删除归档的桶不满足删除时间条件，直接返回不清理
        if not self.is_meet_delete_time(bucket):
            return

        pool_name = bucket.get_pool_name()
        try:
            while True:
                ho = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id='')
                objs = self.get_objs_and_dirs(model_class=model_class)
                if objs is None or len(objs) <= 0:
                    break

                for obj in objs:
                    if obj.is_file():
                        obj_key = obj.get_obj_key(bucket.id)
                        ho.reset_obj_id_and_size(obj_id=obj_key, obj_size=obj.si)
                        ok, err = ho.delete(obj_size=obj.si)
                        if ok:
                            obj.delete()
                        else:
                            self.stdout.write(self.style.WARNING(
                                f"Failed to deleted a object from ceph:" + err))
                    else:
                        obj.delete()

                self.stdout.write(self.style.SUCCESS(
                    f"Success deleted {objs.count()} objects from bucket {bucket.name}."))

            # 如果bucket对应表没有对象了，删除bucket和表
            if model_class.objects.filter(fod=True).count() == 0:
                # 如果有多部份上传未清理，不能删除桶
                if self.has_multipart_upload(bucket):
                    self.stdout.write(self.style.WARNING(
                        f"Ok clear bucket({bucket.name}), but not delete bucket, has multipart upload need to clear."))
                    return

                ok = delete_table_for_model_class(model_class)       # delete bucket table
                if ok and self.delete_bucket_and_part_table(bucket):
                    self.stdout.write(self.style.WARNING(f"deleted bucket and it's table, part table:{bucket.name}"))
                    self.stdout.write(self.style.SUCCESS('Clearing bucket named {0} is completed'.format(bucket.name)))
                else:
                    self.stdout.write(self.style.ERROR(f'deleted bucket table error:{bucket.name}'))
        except (ProgrammingError, Exception) as e:
            self.stdout.write(self.style.ERROR(f'err=({e}) e.args: {e.args}'))
            if e.args[0] == 1146:  # table not exists
                if self.delete_bucket_and_part_table(bucket):
                    self.stdout.write(self.style.WARNING(f"deleted bucket and it's table, part table:{bucket.name}"))
                    self.stdout.write(self.style.SUCCESS('Clearing bucket named {0} is completed'.format(bucket.name)))
            else:
                self.stdout.write(self.style.ERROR(f'deleted bucket({bucket.name}) error: {e}'))

    @staticmethod
    def delete_bucket_and_part_table(bucket):
        parts_table_name = bucket.get_parts_table_name()
        parts_model_class = get_parts_model_class(parts_table_name)
        ok = delete_table_for_model_class(parts_model_class)  # delete parts table
        if ok:
            bucket.delete()
            return True

        return False

    def clear_buckets(self, buckets):
        """
        多线程清理bucket
        :param buckets:
        :return: None
        """
        for bucket in buckets:
            if self.pool_sem.acquire():     # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
                worker = threading.Thread(target=self.thread_clear_one_bucket, kwargs={'bucket': bucket})
                worker.start()

        # 等待所有线程结束
        while True:
            c = threading.active_count()
            if c <= 1:
                break

        self.stdout.write(self.style.SUCCESS('Successfully clear {0} buckets'.format(buckets.count())))

    @staticmethod
    def get_multipart_queryset(bucket: Archive):
        lookups = {}
        lookups['bucket_name'] = bucket.name
        lookups['bucket_id'] = bucket.original_id
        return MultipartUpload.objects.filter(**lookups).all()

    def has_multipart_upload(self, bucket: Archive):
        qs = self.get_multipart_queryset(bucket)
        return qs.exists()
