from django.shortcuts import render
from rest_framework.views import APIView
# Create your views here.
from .serializers import UCISerializer, SinadefSerializer
from rest_framework.response import Response
from rest_framework import status
from .models import DB_uci, DB_sinadef
from io import StringIO
from django.core.management import call_command
from django.test import TestCase


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


class UpdateOpenCovid2(APIView):

    def get(self, request):
        out = StringIO()
        out2 = StringIO()
        args = ['v2']
        argsupd = ['last']
        try:
            call_command('worker_extractor', verbosity=0, *args, stdout=out)
            call_command('worker_update_all', verbosity=0,
                         *argsupd, stdout=out2)
            return Response(out2.getvalue(), status=status.HTTP_200_OK)
        except:
            return Response('Error while running command', status=status.HTTP_400_BAD_REQUEST)

class UpdateDownloads(APIView):

    def get(self, request):
        out = StringIO()
        args = ['v2']
        try:
            call_command('worker_extractor', verbosity=0, *args, stdout=out)
            return Response(out.getvalue(), status=status.HTTP_200_OK)
        except:
            return Response('Error while running command', status=status.HTTP_400_BAD_REQUEST)
