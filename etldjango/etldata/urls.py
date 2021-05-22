from django.urls import path
from . import views

app_name = 'etldata'

urlpatterns = [
    path('updateopencovid/', views.UpdateOpenCovid2.as_view(), name='update_db'),
    path('updatefiles/', views.UpdateDownloads.as_view(), name='updatefiles'),
    path('updatedailyreport/', views.UpdateDailyReport.as_view(), name='updateReport'),
    
    ]
