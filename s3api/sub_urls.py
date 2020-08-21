from django.urls import path, include

from .routers import NoDetailRouter
from . import sub_views


router = NoDetailRouter(trailing_slash=False)
router.register(r'', sub_views.BucketViewSet, basename='bucket')
router.register(r'.+', sub_views.ObjViewSet, basename='obj')


urlpatterns = [
    path('', include(router.urls)),
]
