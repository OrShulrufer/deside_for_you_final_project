from django.shortcuts import redirect
from django.http import HttpResponse, JsonResponse
from auth_helpers import *
from django.conf import settings

# Required views for Google+ authentication   
def revoke(request):
    try:
        token = request.META['HTTP_TOKEN']
        revoke_token(token)
        return JsonResponse({'success':'token revoked'})
    except KeyError:
        return JsonResponse({'error': 'TOKEN header not found'})


def oauth2_callback(request):
    print request
    code = request.GET.get('code', '')
    if code == '':
        token = 'denied'
    else:
        token = handle_callback(code)
    client_location = request.GET.get('state', '')
    if client_location == '':
        client_location = settings.REDIRECT_URI
    uri = client_location + '?token='+token
    #return JsonResponse({'uri':uri})
    return redirect(uri)

def login(request):
    try:
        token = request.META['HTTP_TOKEN']
        client_location = request.META['HTTP_REDIRECT']
        if client_location == '':
            client_location = settings.REDIRECT_URI
    except KeyError:
        return JsonResponse({'login url': handle_redirect(client_location)})
    if check_auth(token):
        # User has been authenticated, form response here.
        return JsonResponse(me(token))
    else:
        # Required redirect
        return JsonResponse({'login url': handle_redirect(client_location)})

def receive_token(request):
    token = request.GET.get('token','')
    return JsonResponse({'token':token})



    
    
    
