from django.urls import path
from . import views

app_name = 'etldata'

urlpatterns = [
    path('uci/', views.Uci_api.as_view(), name='etldata_uci'),
    path('sinadef/', views.Sinadef_api.as_view(), name='etldata_sinadef')
]
