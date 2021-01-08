from django.core.management.base import BaseCommand, CommandError
from rest_framework import serializers

from s3api.managers import MultipartUploadManager
from buckets.models import Bucket


class MultipartUploadsSerializer(serializers.Serializer):
    id = serializers.CharField()
    bucket_name = serializers.CharField()
    obj_id = serializers.IntegerField()
    obj_key = serializers.CharField()
    create_time = serializers.DateTimeField()
    expire_time = serializers.DateTimeField()
    status = serializers.SerializerMethodField(method_name='get_status')

    @staticmethod
    def get_status(obj):
        return obj.get_status_display()


class Command(BaseCommand):
    """
    list多部分上传
    """

    help = """** manage.py list_multipart_upload -h **"""

    def add_arguments(self, parser):
        parser.add_argument(
            '--show-count', default=False, nargs='?', dest='show_count', type=bool, const=True,  # 当命令行有此参数时取值const, 否则取值default
            help='Show the count of multipart upload.',
        )
        parser.add_argument(
            '--bucket', default='', dest='bucket_name', type=str, required=True,
            help='The multipart upload belonging to this bucket will be list.',
        )
        parser.add_argument(
            '--prefix', default='', dest='prefix', type=str,
            help='The prefix of object key',
        )
        parser.add_argument(
            '--offset', default=0, dest='offset', type=int,
            help='The start index of multipart upload will be list.',
        )
        parser.add_argument(
            '--max-num', default=100, dest='max_num', type=int,
            help='The max number of multipart upload will be list.',
        )

    def handle(self, *args, **options):
        bucket_name = options['bucket_name']
        max_num = options['max_num']
        prefix = options['prefix']
        show_count = options['show_count']
        offset = options['offset']
        limit = offset + max_num

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            raise CommandError("Bucket not found.")

        queryset = MultipartUploadManager().list_multipart_uploads_queryset(bucket_name=bucket_name, prefix=prefix)
        ups = queryset[offset:limit]
        serializer = MultipartUploadsSerializer(instance=ups, many=True)
        i = 0
        for up in serializer.data:
            i = i + 1
            if i % 2 == 0:
                self.stdout.write(self.style.SUCCESS(f'{up}'))
            else:
                self.stdout.write(self.style.WARNING(f'{up}'))

        self.stdout.write(self.style.SUCCESS(f'The listed count: {i}.'))
        if show_count:
            count = queryset.count()
            self.stdout.write(self.style.SUCCESS(f'The count of all upload: {count}.'))

