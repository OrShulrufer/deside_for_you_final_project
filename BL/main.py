'''
from pyspark.sql import SparkSession
from collaborative_filtering import CF
from create_clusters import Cluster
from prepare_data_spark import Data
from content_base_filtering import CBF
from NoCourses import NoCourses
from test import Test

print("Starting create classes")
# creating classes
prep = Data()
cluster = Cluster()
cf = CF()
cbf = CBF()
test = Test()
no_courses = NoCourses()

print("Starting setup Spark")
# Create Spark Session
spark = SparkSession.builder.appName("SQLBasicResultexample"). \
    config("spark.some.config.option", "some-value"). \
    master("local[2]"). \
    getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Starting loading files")
# loading necessary files
df_students = spark.read.csv("data/train_after_prepare.csv", inferSchema="true", header="true")
df_courses = spark.read.csv("data/course_descriptions.csv", inferSchema="true", header="true")

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
    return no_courses.get_courses(df_courses, "Education & Teaching", spark)

'''
if __name__== "__main__":
    print(get_courses_by_user_id("MHxPC130442623"))
    print(get_courses_by_topic("Education & Teaching"))
'''
def get_course_by_id(course_number):
    df_courses.toPandas().loc[df_courses.toPandas()['course_number'] == course_number]
'''
