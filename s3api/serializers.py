from rest_framework import serializers


class BucketListSerializer(serializers.Serializer):
    """
    桶列表序列化器
    """
    CreationDate = serializers.SerializerMethodField(method_name='get_creation_date')
    Name = serializers.SerializerMethodField(method_name='get_name')

    @staticmethod
    def get_creation_date(obj):
        return serializers.DateTimeField().to_representation(obj.created_time)

    @staticmethod
    def get_name(obj):
        return obj.name

