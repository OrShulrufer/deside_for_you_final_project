
from flask import Flask, request, send_from_directory, jsonify
from json import JSONEncoder

app = Flask(__name__,static_url_path='')

# from UserController import validate_id_token #currently not working in Or's pc
app.config.from_pyfile('config.py')  # debug=True cause to restart this file several times
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from BL.collaborative_filtering import CF
from BL.create_clusters import Cluster
from BL.prepare_data_spark import Data
from BL.content_base_filtering import CBF
from BL.NoCourses import NoCourses
from tests.test import Test

print("Starting create classes")



# creating classes
prep = Data()
cluster = Cluster()
cf = CF()
cbf = CBF()
test = Test()

no_courses = NoCourses()


print("Starting setup Spark")

conf = SparkConf()
conf.set('spark.logConf', app.config["SPARK_LOG_CONF"])
conf.set("spark.jars", app.config["MY_SQL_CONNECTOR_PATH"])

# Create Spark Session
spark = SparkSession.builder.appName("DecideForYou").config(conf=conf).master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


print("Starting loading files")
# loading necessary files
df_students = spark.read.csv("BL/data/train_after_prepare.csv", inferSchema="true", header="true")
df_courses = spark.read.csv("BL/data/course_descriptions.csv", inferSchema="true", header="true")

print("Starting putting waits")
# put waits
df_students_after_waits = prep.put_waits(df_students, 2, 2, 2)

print("Starting clustering")
# crete clusters
df_students_after_clustering = cluster.generate_clasters(df_students_after_waits, spark)


def get_courses_by_user_id(user_id):
    print("Starting  collaborative filtering")
    courses = cf.get_curses(df_students_after_clustering, user_id, spark)
    print("Starting  content based filtering")
    courses += cbf.get_courses(df_students_after_clustering, df_courses, user_id, spark)
    return courses

def get_courses_by_topic(topic):
    print("starting getting courses by topic")
    return no_courses.get_courses(df_courses, topic, spark)



'''
@app.route('/<path:path>')
def send_files(path):
    return send_from_directory('static', path)
'''

'''cehck if this works - redirect to other page if not '/'
@app.route('/', defaults={'pagenotfound.html': ''})
@app.route('/<path:u_path>')
def start():
    return send_from_directory('static', 'index.html')
'''


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
        self.__init__(df_row["Institution"],df_row["course_number"],df_row["Course Title"],df_row["Description"],df_row["Topics"])


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
        course= CourseDetails();
        course.set_from_df_row(row);
        returned_list.append(MyEncoder().encode(course))
    return returned_list

@app.route('/getPerson')
def get_hello():
    p1 = Person("John", 36)
    p2 = Person("Dan", 24)
    my_list=[MyEncoder().encode(p1),MyEncoder().encode(p2)];
    return jsonify(results=my_list);


@app.route('/googleAuth/<token_id>')
def google_auth(token_id):
    # validate_id_token(token_id) #currently not working in Or's pc
    return "hello";

'''
The data past the ? indicate it's a query parameter, so they won't be included in that patter. You can either access that part form request.query_string or build it back up from the request.args as mentioned in the comments
'''
@app.route('/getCourses') #'/getCourses/<path:userId>'
def get_courses():
    userId = request.args.get('userId');
    topic = request.args.get('topic');
    courses_id_df=None
    if userId is not None and userId != "":
        courses_id_df=get_courses_by_user_id(userId)
    else:
        if topic is not None and topic != "":
            courses_id_df = get_courses_by_topic(topic)
    if courses_id_df is None:
        return None

    filtered_df = df_courses.toPandas().loc[df_courses.toPandas()['course_number'].isin(courses_id_df)]

    '''
    print(filtered_df)
    returned_list=[]
    for index, row in filtered_df.iterrows():
        course= CourseDetails();
        course.set_from_df_row(row);
        returned_list.append(MyEncoder().encode(course))
    '''

    return jsonify(results=df_to_courses_details(filtered_df))


if __name__ == '__main__':
    app.run()




