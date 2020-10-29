
from google.oauth2 import id_token
from google.auth.transport import requests

CLIENT_ID = '1042886129954-8bmoaf2kfg7q4vjoohto0if4a0h741ii.apps.googleusercontent.com'

def validate_id_token(token):
    try:
        # Specify the CLIENT_ID of the app that accesses the backend:
        idinfo = id_token.verify_oauth2_token(token, requests.Request(), CLIENT_ID)

        # Or, if multiple clients access the backend server:
        # idinfo = id_token.verify_oauth2_token(token, requests.Request())
        # if idinfo['aud'] not in [CLIENT_ID_1, CLIENT_ID_2, CLIENT_ID_3]:
        #     raise ValueError('Could not verify audience.')

        if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
            raise ValueError('Wrong issuer.')

        # If auth request is from a G Suite domain:
        # if idinfo['hd'] != GSUITE_DOMAIN_NAME:
        #     raise ValueError('Wrong hosted domain.')

        # ID token is valid. Get the user's Google Account ID from the decoded token.
        userid = idinfo['sub']

    except ValueError:
        # Invalid token
        pass


class User:
    def __init__(self, user_name, course_number=None,course_title=None,description=None, topics=None):
        self.course_number = course_number
        self.course_title = course_title
        self.description = description
        self.topics = topics

class UserController():

    def __init__(self,token_id):
        self.token_id=token_id

