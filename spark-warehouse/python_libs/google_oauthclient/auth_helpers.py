from django.contrib.auth.models import User
from django.conf import settings
from oauth2client.client import OAuth2WebServerFlow, FlowExchangeError
import urllib
import json

def me(token):
    try:
        uri = 'https://www.googleapis.com/plus/v1/people/me?access_token=' + token
        r = urllib.urlopen(uri)
        u = json.loads(r.read())
        return u
    except:
        return False
    

def check_auth(token):
    gmail = get_gmail(token)
    if not gmail:
        return False
    else:
        try:
            user = User.objects.get(email=gmail) 
            return True
        except User.DoesNotExist:
            return False

def get_gmail(token):
    uri = 'https://www.googleapis.com/plus/v1/people/me?access_token=' + token
    r = urllib.urlopen(uri)
    try:
        gmail = json.loads(r.read())['emails'][0]['value']
        return gmail
    except KeyError:
        return False

def handle_redirect(client_location):
    flow = OAuth2WebServerFlow(client_id=settings.CLIENT_ID,client_secret=settings.CLIENT_SECRET, scope=settings.SCOPES,redirect_uri=settings.CALLBACK_URI,state=client_location)
    return flow.step1_get_authorize_url()

def handle_callback(code):
    try:
        #flow = OAuth2WebServerFlow(client_id=settings.CLIENT_ID,client_secret=settings.CLIENT_SECRET, scope=settings.SCOPES,redirect_uri=settings.CALLBACK_URI)
        #credentials = flow.step2_exchange(code)
        
        uri_root = 'https://www.googleapis.com/oauth2/v3/token'
        params = urllib.urlencode({'client_id':settings.CLIENT_ID,
                'client_secret':settings.CLIENT_SECRET,
                'redirect_uri':settings.CALLBACK_URI,
                'code':code,
                'grant_type':'authorization_code'})
        response = urllib.urlopen(url=uri_root,data=params).read()
        try:
            access_token = json.loads(response)['access_token']
            print json.loads(response)
            gmail = get_gmail(access_token)
            try:
                user = User.objects.get(email=gmail)
            except User.DoesNotExist:
                user = User.objects.create_user(gmail, gmail, 'blank')
                user.save()
            #request.session['token'] = credentials.access_token
            return access_token
        except KeyError:
            return 'denied'
    except FlowExchangeError:
        return 'denied'

        
def revoke_token(token):
    uri = 'https://accounts.google.com/o/oauth2/revoke?token=' + token
    urllib.urlopen(uri)
    return None
