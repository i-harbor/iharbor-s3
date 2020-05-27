import hmac
import base64
from hashlib import sha256

from django.utils.translation import gettext as _
from rest_framework.authentication import BaseAuthentication, get_authorization_header
from . import exceptions


AWS4_HMAC_SHA256 = 'AWS4-HMAC-SHA256'


class S3V4Authentication(BaseAuthentication):
    """
    S3 v4 based authentication.

    Clients should authenticate by passing the token key in the "Authorization"
    HTTP header, prepended with the string "AWS4-HMAC-SHA256 ".  For example:

        Authorization: AWS4-HMAC-SHA256 Credential=xxx,SignedHeaders=xxx,Signature=xxx
    """

    keyword = AWS4_HMAC_SHA256
    model = None

    def get_model(self):
        if self.model is not None:
            return self.model
        from users.models import AuthKey
        return AuthKey

    def authenticate(self, request):
        auth = get_authorization_header(request).split(maxsplit=1)

        if not auth or auth[0].lower() != self.keyword.lower().encode():
            return None

        if len(auth) == 1:
            msg = _('Invalid auth header. No credentials provided.')
            raise exceptions.S3AuthorizationHeaderMalformed(msg)

        try:
            auth_key_str = auth[1].decode()
        except UnicodeError:
            msg = _('Invalid auth header. Auth string should not contain invalid characters.')
            raise exceptions.S3AuthorizationHeaderMalformed(msg)

        return self.authenticate_credentials(request, auth_key_str)

    def authenticate_credentials(self, request, auth_key_str):
        auths = self.parse_auth_key_string(auth_key_str)
        credential = auths.get('Credential')
        signed_headers = auths.get('SignedHeaders')
        signature = auths.get('Signature')

        access_key = credential.split('/')[0]

        model = self.get_model()
        try:
            auth_key = model.objects.select_related('user').get(id=access_key)
        except model.DoesNotExist:
            raise exceptions.S3InvalidAccessKeyId()

        if not auth_key.user.is_active:
            raise exceptions.S3InvalidAccessKeyId(_('User inactive or deleted.'))

        # 是否未激活暂停使用
        if not auth_key.is_key_active():
            raise exceptions.S3InvalidAccessKeyId(_('Invalid access_key. Key is inactive and unavailable'))

        # 验证加密signature
        # if generate_signature(auth_key.secret_key) != signature:
        #     raise exceptions.S3AuthorizationHeaderMalformed()

        return auth_key.user, auth_key  # request.user, request.auth

    @staticmethod
    def parse_auth_key_string(auth_key):
        auth = auth_key.split(',')
        if len(auth) != 3:
            raise exceptions.S3AuthorizationHeaderMalformed()

        ret = {}
        for a in auth:
            a = a.strip(' ')
            name, val = a.split('=', maxsplit=1)
            if name not in ['Credential', 'SignedHeaders', 'Signature']:
                raise exceptions.S3AuthorizationHeaderMalformed()
            ret[name] = val

        return ret

    def authenticate_header(self, request):
        return self.keyword


