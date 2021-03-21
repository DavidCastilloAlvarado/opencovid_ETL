from django.shortcuts import render
from rest_framework.views import APIView
# Create your views here.
from .serializers import UCISerializer, SinadefSerializer
from rest_framework.response import Response
from rest_framework import status
from .models import DB_uci, DB_sinadef


class Uci_api(APIView):
    # api de prueba
    serializer_class = UCISerializer

    def get(self, request, format=None):
        """"Return the whole list of customer allocated in our database """
        queryset = DB_uci.objects.all()[:100]
        queryset = self.serializer_class(queryset, many=True)
        return Response(queryset.data)

    def post(self, request):
        serializer = UCISerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class Sinadef_api(APIView):
    # api de prueba
    serializer_class = SinadefSerializer

    def get(self, request, format=None):
        """"Return the whole list of customer allocated in our database """
        queryset = DB_sinadef.objects.all()[:300]
        queryset = self.serializer_class(queryset, many=True)
        return Response(queryset.data)

    def post(self, request):
        serializer = SinadefSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
