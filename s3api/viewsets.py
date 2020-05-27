from django.conf import settings
from django.utils import timezone
from django.http import Http404
from django.core.exceptions import PermissionDenied
from rest_framework.viewsets import GenericViewSet
from rest_framework import status
from rest_framework.views import set_rollback
from rest_framework.response import Response
from rest_framework.exceptions import (APIException, NotAuthenticated, AuthenticationFailed)

from . import exceptions
from .renders import CusXMLRenderer


def exception_handler(exc, context):
    """
    Returns the response that should be used for any given exception.

    By default we handle the REST framework `APIException`, and also
    Django's built-in `Http404` and `PermissionDenied` exceptions.

    Any unhandled exceptions may return `None`, which will cause a 500 error
    to be raised.
    """
    if isinstance(exc, Http404):
        exc = exceptions.S3NotFound()
    elif isinstance(exc, PermissionDenied):
        exc = exceptions.S3AccessDenied()

    if isinstance(exc, APIException):
        headers = {}
        if getattr(exc, 'auth_header', None):
            headers['WWW-Authenticate'] = exc.auth_header
        if getattr(exc, 'wait', None):
            headers['Retry-After'] = '%d' % exc.wait

        if isinstance(exc.detail, (list, dict)):
            data = exc.detail
        else:
            data = {'detail': exc.detail}

        set_rollback()
        return Response(data, status=exc.status_code, headers=headers)

    if isinstance(exc, exceptions.S3Error):
        set_rollback()
        return Response(exc.err_data(), status=exc.status_code)

    return None


class CustomGenericViewSet(GenericViewSet):
    """
    自定义GenericViewSet类，重写get_serializer方法，以通过context参数传递自定义参数
    """
    def get_serializer(self, *args, **kwargs):
        """
        Return the serializer instance that should be used for validating and
        deserializing input, and for serializing output.
        """
        serializer_class = self.get_serializer_class()
        context = self.get_serializer_context()
        context.update(kwargs.get('context', {}))
        kwargs['context'] = context
        return serializer_class(*args, **kwargs)

    def perform_authentication(self, request):
        super().perform_authentication(request)

        # 用户最后活跃日期
        user = request.user
        if user.id and user.id > 0:
            try:
                date = timezone.now().date()
                if user.last_active < date:
                    user.last_active = date
                    user.save(update_fields=['last_active'])
            except:
                pass

    @staticmethod
    def get_bucket_name(request):
        """
        从域名host中取bucket name

        :return: str
            bucket name     # BucketName.SERVER_HTTP_HOST_NAME
            ''              # SERVER_HTTP_HOST_NAME or other

        """
        main_host = getattr(settings, 'SERVER_HTTP_HOST_NAME', 'obs.cstcloud.cn')
        host = request.get_host()
        if host.endswith('.' + main_host):
            bucket_name, _ = host.split('.', maxsplit=1)
            return bucket_name

        return ''

    @staticmethod
    def get_obj_path_name(request):
        """
        从url path中获取对象key

        :return: str
        """
        key: str = request.path
        return key.strip('/')

    def get_renderers(self):
        """
        renderer_classes列表项 可以是类或对象
        """
        return [renderer() if renderer is not object else renderer for renderer in self.renderer_classes]

    def set_renderer(self, request, renderer):
        """
        设置渲染器

        :param request: 请求对象
        :param renderer: 渲染器对象
        :return:
        """
        request.accepted_renderer = renderer
        request.accepted_media_type = renderer.media_type

    def handle_exception(self, exc):
        """
        Handle any exception that occurs, by returning an appropriate response,
        or re-raising the error.
        """
        if isinstance(exc, (NotAuthenticated,
                            AuthenticationFailed)):
            # WWW-Authenticate header for 401 responses, else coerce to 403
            auth_header = self.get_authenticate_header(self.request)

            if auth_header:
                exc.auth_header = auth_header
            else:
                exc.status_code = status.HTTP_403_FORBIDDEN

        exception_handler = self.get_exception_handler()

        context = self.get_exception_handler_context()
        response = exception_handler(exc, context)

        if response is None:
            self.raise_uncaught_exception(exc)

        response.exception = True
        self.set_renderer(self.request, CusXMLRenderer(root_tag_name='Error'))
        return response
