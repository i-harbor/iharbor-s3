from django.apps import AppConfig
from django.core.checks import register

from . import checks


class S3ApiConfig(AppConfig):
    name = 's3api'

    def ready(self):
        register(checks.check_ceph_settins)
