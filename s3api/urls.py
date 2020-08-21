from django.urls import path, include

from .routers import NoDetailRouter
from . import views


router = NoDetailRouter(trailing_slash=False)
router.register(r'', views.MainHostViewSet, basename='bucket')


urlpatterns = [
    path('', include(router.urls)),
]
