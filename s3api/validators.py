import re

from django.utils.translation import gettext as _
from rest_framework.serializers import ValidationError

from buckets.models import Bucket, BucketLimitConfig


dns_regex = re.compile(r'(?!-)'             # can't start with a -
                       r'[a-zA-Z0-9-]{,63}'
                       r'(?<!-)$')          # can't end with a dash

LABEL_RE = re.compile(r'[a-z0-9][a-z0-9\-]*[a-z0-9]')


def DNSStringValidator(value):
    """
    验证字符串是否符合NDS标准
    """
    if not dns_regex.match(value):
        raise ValidationError(_('字符串不符合DNS标准'))


def bucket_limit_validator(user):
    """
    验证用户拥有的存储桶数量是否达到上限
    :param user: 无
    """
    count = Bucket.get_user_valid_bucket_count(user=user)
    limit = BucketLimitConfig.get_user_bucket_limit(user=user)
    if count >= limit:
        raise ValidationError(_('您可以拥有的存储桶数量已达上限'))


def check_dns_name(bucket_name):
    """
    Check to see if the ``bucket_name`` complies with the
    restricted DNS naming conventions necessary to allow
    access via virtual-hosting style.

    Even though "." characters are perfectly valid in this DNS
    naming scheme, we are going to punt on any name containing a
    "." character because these will cause SSL cert validation
    problems if we try to use virtual-hosting style addressing.
    """
    if '.' in bucket_name:
        return False
    n = len(bucket_name)
    if n < 3 or n > 63:
        # Wrong length
        return False
    match = LABEL_RE.match(bucket_name)
    if match is None or match.end() != len(bucket_name):
        return False
    return True
