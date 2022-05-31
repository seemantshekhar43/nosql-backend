from django.urls import path

from api import views

urlpatterns = [
    path('', views.get_table),
    # path('docs', views.get_docs),
]
