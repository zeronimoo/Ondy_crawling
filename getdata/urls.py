from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('getdata/', views.combined_crawling_view, name='getdata'),
]