from django.shortcuts import render
from rest_framework.views import APIView
# Create your views here.
from .serializers import ReportManualMinsaSerializer
from rest_framework.response import Response
from rest_framework import status
from .models import DB_uci, DB_sinadef
from io import StringIO
from django.core.management import call_command
from django.test import TestCase

class UpdateOpenCovid2(APIView):

    def get(self, request):
        out = StringIO()
        out2 = StringIO()
        out3 = StringIO()
        args = ['v2']
        argsupd = ['last']
        try:
            call_command('worker_extractor', verbosity=0, *args, stdout=out)
            call_command('worker_update_all', verbosity=0,
                         *argsupd, stdout=out2)
            call_command('worker_t_resumen', verbosity=0, stdout=out3)
            return Response(out3.getvalue(), status=status.HTTP_200_OK)
        except:
            return Response('Error while running command', status=status.HTTP_503_SERVICE_UNAVAILABLE)

class UpdateDownloads(APIView):
    def get(self, request):
        out = StringIO()
        args = ['v2']
        try:
            call_command('worker_extractor', verbosity=0, *args, stdout=out)
            return Response(out.getvalue(), status=status.HTTP_200_OK)
        except:
            return Response('Error while running command', status=status.HTTP_503_SERVICE_UNAVAILABLE)

class UpdateDailyReport(APIView):
    post_serializer = ReportManualMinsaSerializer
    def get(self, request):
        out = StringIO()
        try:
            call_command('worker_daily', verbosity=0, stdout=out)
            return Response(out.getvalue(), status=status.HTTP_200_OK)
        except:
            return Response('Error while running command', status=status.HTTP_503_SERVICE_UNAVAILABLE)

    def post(self, request, format=None):
        """
        upload data manualy
        """
        serializer = self.post_serializer(data=request.data)
        if serializer.is_valid():
            serializer.save()


