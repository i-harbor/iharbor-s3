from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated

from .renders import CusXMLRenderer
from .viewsets import CustomGenericViewSet
from buckets.models import Bucket
from .serializers import BucketListSerializer


class MainHostViewSet(CustomGenericViewSet):
    """
    主域名请求视图集
    """
    permission_classes = []

    def list(self, request, *args, **kwargs):
        """
        list buckets

        HTTP/1.1 200
        <?xml version="1.0" encoding="UTF-8"?>
        <ListBucketsOutput>
           <Buckets>
              <Bucket>
                 <CreationDate>timestamp</CreationDate>
                 <Name>string</Name>
              </Bucket>
           </Buckets>
           <Owner>
              <DisplayName>string</DisplayName>
              <ID>string</ID>
           </Owner>
        </ListBucketsOutput>
        """
        user = request.user
        buckets_qs = Bucket.objects.filter(user=user).all()    # user's own
        serializer = BucketListSerializer(buckets_qs, many=True)

        # xml渲染器
        self.set_renderer(request, CusXMLRenderer(root_tag_name='ListBucketsOutput', item_tag_name='Bucket'))
        return Response(data={
            'Buckets': serializer.data,
            'Owner': {'DisplayName': user.username, 'ID': user.id}
        }, status=status.HTTP_200_OK)

