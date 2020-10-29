from django.conf.urls import url
from google_oauthclient import views

urlpatterns = [
    url(r'^login', views.login),
    url(r'^revoke',views.revoke, name='revoke'),
    url(r'^oauth2callback', views.oauth2_callback, name='oauth2callback'),
    url(r'^receive_token', views.receive_token, name='receive_token')
]
