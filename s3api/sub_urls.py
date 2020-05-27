from django.urls import path, include

from .routers import NoDetailRouter
from . import sub_views


router = NoDetailRouter(trailing_slash=False)
router.register(r'', sub_views.BucketViewSet, base_name='bucket')
router.register(r'.+', sub_views.ObjViewSet, base_name='obj')


urlpatterns = [
    path('', include(router.urls)),
]
