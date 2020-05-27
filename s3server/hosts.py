from django.conf import settings
from django_hosts import patterns, host

host_patterns = patterns(
    '',
    host(r's3.obs', settings.ROOT_URLCONF, name='default'),
    host(r'(\w+).s3.obs', 's3server.sub_urls', name='sub_url'),
)
