from flask import Flask, render_template,request, send_from_directory, jsonify
from json import JSONEncoder
from DataAccess import get_topics, get_courses_some_cols,get_courses_details,get_countries,update_user,\
    update_user_courses, get_courses, get_users_same_cluster,insert_new_course_to_system
import threading
from threading import Timer
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from BL.create_clusters import Cluster
from BL import columns_names
from BL.hybrid import Hybrid
from BL.data_matrix import DataMatrix
from datetime import datetime

app = Flask(__name__, static_url_path='')
# from UserController import validate_id_token #currently not working in Or's pc
from UserController import check_user,insert_and_get_user,get_the_user, get_user_allowed_rec_response
app.config.from_pyfile('config.py')  # debug=True cause to restart this file several times


print("Starting setup Spark")


# trying threads
def task():
    threading.Timer(3600, task).start()
    if datetime.now().hour == 2:
        predictions_df = Cluster.rearrange_clusters()
        Cluster.update_db_clusters(predictions_df, None)

task()  # starting the task

def get_courses_by_user_id(user_id):
    user_course_df = get_users_same_cluster(user_id)

    clf_matrix = DataMatrix.get_data_matrix(user_course_df)

    # get all courses table
    courses_df = get_courses()
    return Hybrid.get_courses(clf_matrix, courses_df, user_id), courses_df

def get_courses_by_topic(topic):
    print("starting getting courses by topic")
    return get_courses(topic, None, False, None, 10)



@app.route('/')
def start():
    return send_from_directory('static', 'index.html')


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

class CourseDetails:
    def __init__(self, institution=None, course_number=None,course_title=None,description=None, topics=None):
        self.institution = institution
        self.course_number = course_number
        self.course_title = course_title
        self.description = description
        self.topics = topics

    def set_from_df_row(self,df_row):
        self.__init__(df_row[columns_names.institution],
                      df_row[columns_names.course_number],
                      df_row[columns_names.course_title],
                      df_row[columns_names.description],df_row[columns_names.topics])


class MyEncoder(JSONEncoder):
        def default(self, o):
            return o.__dict__

'''
To select rows whose column value equals a scalar, some_value, use ==:

df.loc[df['column_name'] == some_value]
'''

def df_to_courses_details(df):
    returned_list=[]
    for index, row in df.iterrows():
        course= CourseDetails()
        course.set_from_df_row(row)
        returned_list.append(MyEncoder().encode(course))
    return returned_list


def get_single_course_dict(institution,course_number,course_title,description,topics, web_link):
    course = {
            "institution":  institution,
            "course_number": course_number,
            "course_title":  course_title,
            "description":  description,
            "topics":  topics,
            "web_link" : web_link
        }
    return course

def courses_df_to_response_list(courses_df):
    returned_list=[]
    for index, row in courses_df.iterrows():
        course = get_single_course_dict(row[columns_names.institution],
                               row[columns_names.course_number],
                               row[columns_names.course_title],
                               row[columns_names.description],
                               row[columns_names.topics])
        returned_list.append(course)
    return returned_list


def course_list_to_response_list(courses_list):
    returned_list=[]
    for course in courses_list:
        returned_list.append(get_single_course_dict(course[columns_names.institution],
                                course[columns_names.course_number],
                                course[columns_names.course_title],
                                course[columns_names.description],
                                course[columns_names.topics],
                                course[columns_names.web_link]))
    return returned_list

@app.route('/getPerson')
def get_hello():
    p1 = Person("John", 36)
    p2 = Person("Dan", 24)
    my_list = [MyEncoder().encode(p1),MyEncoder().encode(p2)]
    return jsonify(results=my_list)


@app.route('/getTopics')
def get_topics_list():
    topics = get_topics()
    my_list= [row['topic'] for row in topics]
    return jsonify(results=my_list)

@app.route('/getAllCourses')
def get_courses_list():
    my_list= get_courses_some_cols()
    return jsonify(results=my_list)


@app.route('/getCoursesDetails')
def get_courses_details_list():
    my_list = get_courses_details()
    return jsonify(results=my_list)


@app.route("/addNewCourse", methods=['POST'])
def add_new_course():
    dt_obj = datetime.now()
    new_course_num="{}{}{}{}{}{}".\
        format(dt_obj.year, dt_obj.month,dt_obj.day,dt_obj.hour,dt_obj.minute,dt_obj.second)
    obj = request.json
    insert_new_course_to_system(new_course_num,"" if obj['institution'] is None else obj['institution'],
                                obj['title'],obj['topicsStr'],obj['description'], obj['web_link'])
    return "insert course success"


@app.route('/getCountries')
def get_countries_list():
    return jsonify(results=get_countries());


@app.route('/validateUser/<is_signin>/<email>/<password>/<user_name>')
def validate_user(is_signin,email, password, user_name):
    if 'MHxPC' in email:
        result = check_user(email, None)
    elif is_signin == "true":
        result = check_user(email, password)
    else:
        result = insert_and_get_user(email, password, "" if user_name is None or user_name == "null" else user_name.strip())
    return jsonify(result)


@app.route('/getUser/<email>')
def get_existing_user(email):
    result = get_the_user(email,None)
    return jsonify(result)


@app.route('/googleAuth/<token_id>')
def google_auth(token_id):
    # validate_id_token(token_id) #currently not working in Or's pc
    result = get_the_user('', token_id)
    return jsonify(result)


@app.route("/updateUser", methods=['POST'])
def update_user_profile():
    #start = datetime.now()
    obj = request.json
    update_demographic = obj['updateDemographic']
    email = obj['email']
    update_user(email, "" if obj['user_name'] is None else obj['user_name']
                , obj['education'], obj['YOB'], obj['gender'], obj['country_id'],update_demographic)
    update_user_courses(email,obj['user_courses'])
    #spark_session = get_or_create_SparkSession()
    if update_demographic:
        predictions_sparkdf = Cluster.rearrange_clusters(spark_session, email)
        Cluster.update_db_clusters(predictions_sparkdf, email)
    #end=datetime.now()
    #print('update_user_profile done in time: ', end-start)
    return "something {}".format(obj)


'''
The data past the ? indicate it's a query parameter, so they won't be included in that patter. You can either access that part form request.query_string or build it back up from the request.args as mentioned in the comments
'''


@app.route('/getCourses') #'/getCourses/<path:userId>'
def get_courses_result():
    userId = request.args.get('userId')
    topic = request.args.get('topic')

    if userId is not None and userId != "":
        response_msg,response_code = get_user_allowed_rec_response(userId)
        if response_code != 200:
            return response_msg, response_code

        courses_id_list, courses_df = get_courses_by_user_id(userId)
        filtered_df = courses_df.loc[courses_df[columns_names.course_number].isin(courses_id_list)]
        result_list = courses_df_to_response_list(filtered_df)
    else:
        if topic is not None and topic != "":
            result_list = course_list_to_response_list(get_courses(topic, to_list=True))
        #list(filter(lambda course: course['course_number'] in result_courses_id_list, allCoursesList))

    return jsonify(results=result_list)


if __name__ == '__main__':
    app.run()


