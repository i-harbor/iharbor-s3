from django.urls import path, include


urlpatterns = [
    path('', include('s3api.sub_urls'))
]
