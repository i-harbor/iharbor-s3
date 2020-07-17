from django.core.management.base import BaseCommand, CommandError

from s3api.utils import (create_table_for_model_class, is_model_table_exists, delete_table_for_model_class)
from s3api.managers import get_parts_model_class
from buckets.models import Bucket


class Command(BaseCommand):
    """
    创建或删除多部分上传数据库表
    """

    help = """** manage.py create_bucket_parts_table --bucket-name="s36" **    
           **  manage.py create_bucket_parts_table --bucket-name="s36" --delete ** 
        """

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default='', dest='bucket-name', type=str,
            help='Create parts table for this bucket.',
        )
        parser.add_argument(
            '--delete', default=False, nargs='?', dest='delete', type=bool, const=True,    # 当命令行有此参数时取值const, 否则取值default
            help='The table will be delete if use this argument',
        )

    def handle(self, *args, **options):
        delete = options['delete']
        bucket_name = options['bucket-name']
        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            raise CommandError("Bucket not found.")

        table_name = bucket.get_parts_table_name()
        parts_model_class = get_parts_model_class(table_name=table_name)

        exists = is_model_table_exists(parts_model_class)
        if delete:
            if exists:
                if input('Are you sure to delete the table?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
                    raise CommandError("cancelled.")

                if input("The last chance to go back. It's best to back up your data anyway. Will delete the table.\n\n" + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
                    raise CommandError("cancelled.")

                if delete_table_for_model_class(parts_model_class):
                    self.stdout.write(self.style.SUCCESS('Delete table Successfully.'))
                else:
                    self.stdout.write(self.style.ERROR('Failed to delete the table.'))
            else:
                self.stdout.write(self.style.SUCCESS('The table is not exists.'))
        else:
            if exists:
                self.stdout.write(self.style.SUCCESS('The table already exists'))
            else:
                if input('Are you sure to create the table?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
                    raise CommandError("cancelled.")

                if create_table_for_model_class(parts_model_class):
                    self.stdout.write(self.style.SUCCESS('Create the table Successfully.'))
                else:
                    self.stdout.write(self.style.ERROR('Failed to create the table'))
