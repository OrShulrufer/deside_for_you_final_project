
from google.oauth2 import id_token
from google.auth.transport import requests
from DataAccess import insert_user,get_user,insert_user_to_other_tbls, get_user_courses
import BL.columns_names as columns_names

CLIENT_ID = '902435638274-3q0iokto9629nbbi9f56n5v812t5e5lg.apps.googleusercontent.com'

def validate_id_token(token):
    try:
        # Specify the CLIENT_ID of the app that accesses the backend:
        idinfo = id_token.verify_oauth2_token(token, requests.Request(), CLIENT_ID)
        if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
            raise ValueError('Wrong issuer.')

        # If auth request is from a G Suite domain:
        # if idinfo['hd'] != GSUITE_DOMAIN_NAME:
        #     raise ValueError('Wrong hosted domain.')

        # ID token is valid. Get the user's Google Account ID from the decoded token.
        userid = idinfo['sub']
        return idinfo


    except ValueError:
        # Invalid token
        pass


def insert_and_get_user(email, password, user_name):
    if not insert_user(email, password, user_name):
        return {'response_message': "email already exist in system"}
    insert_user_to_other_tbls(email)
    return get_user(email, password)[0]


def get_the_user(email, token_id=None):
    if token_id is not None:
        google_user = validate_id_token(token_id)
        email = google_user['email']
        insert_user(email, '', google_user['name'], google_user['sub'])
        insert_user_to_other_tbls(email)
    user = check_user(email, None)
    return user


def check_user(email, password):
    users_list = get_user(email, password)
    if len(users_list) is 0:
        return {'response_message': "email or password are incorrect"}
    user = users_list[0]
    courses = get_user_courses(email)
    courses_list = [row['course_id'] for row in courses] if len(courses) > 0 else []
    user["user_courses"] = courses_list
    return user

def get_user_allowed_rec_response(user_id):
    users_list = get_user(user_id)
    if len(users_list) is 0:
        return "user is Unauthorized", 401
    elif users_list[0]["education"] is None:
        return "user must complete details before asking recommendations", 403
    return "", 200

'''
class User:
    def __init__(self, user_name, course_number=None,course_title=None,description=None, topics=None):
        self.course_number = course_number
        self.course_title = course_title
        self.description = description
        self.topics = topics



class UserController():

    def __init__(self,token_id):
        self.token_id=token_id
'''

