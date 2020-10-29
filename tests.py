import pandas as pd
from flask import Flask
from DataAccess import get_users_demogr_details_df
from DataAccess import get_all_course_id_student_id_panda
from DataAccess import get_all_course_id_student_id
from chek_hyrid import CheckHyrid
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from BL.create_clusters import Cluster
from BL.prepare_data_spark import Data
from BL.data_matrix import DataMatrix
from BL.collaborative_filtering import CF
from csv import writer
from check_clf import CheckClf
from check_CBF import  CheckCBF
import os
import os.path
from os import path
from pyspark.sql import SQLContext
import DataAccess

app = Flask(__name__, static_url_path='')
# from UserController import validate_id_token #currently not working in Or's pc
app.config.from_pyfile('config.py')  # debug=True cause to restart this file several times


print("Starting setup Spark")
conf = SparkConf()
conf.set('spark.logConf', app.config["SPARK_LOG_CONF"])
conf.set("spark.jars", app.config["MY_SQL_CONNECTOR_PATH"])

# Create Spark Session
spark = SparkSession.builder.appName("DecideForYou").config(conf=conf).master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

#CheckCBF.check_CBF(1)



df_courses = DataAccess.get_courses()
print(df_courses.columns)

user_course_df = pd.read_csv("train_students_course.csv")

val_df = pd.read_csv("val_students_course.csv")

clf_matrix_df = DataMatrix.get_data_matrix(user_course_df)
print(clf_matrix_df)

spark_df_student_demografic_data = DataAccess.get_users_demogr_details_df(spark)
spark_df_student_demografic_data = spark_df_student_demografic_data.toPandas()
a = user_course_df["userid_DI"].unique()
spark_df_student_demografic_data = spark_df_student_demografic_data[spark_df_student_demografic_data["userid_DI"].isin(a)]
print(spark_df_student_demografic_data)

sqlCtx = SQLContext(spark.sparkContext)
spark_df_student_demografic_data = sqlCtx.createDataFrame(spark_df_student_demografic_data)

df_clusters = CheckHyrid.create_clusters(spark, spark_df_student_demografic_data)
print(df_clusters)

p = CheckHyrid.get_user_courses_recommendation(df_clusters, clf_matrix_df, df_courses, user_course_df, val_df)
print(p)



#print(CheckHyrid.get_percentage_of_success("hybrid_courses_ceck.csv"))
import os
#os.remove("hybrid_courses_ceck.csv")


#print(CheckHyrid.get_percentage_of_success("hybrid_courses_ceck.csv"))


"""
colums = ["user", "courses", "yes", "no"]
df = pd.read_csv("CBF_course3.csv", names=colums)

yes = df['yes'].sum()
no = df['no'].sum()

presantage = (yes / (yes + no)) * 100

print(presantage)
"""
"""
sqlCtx = SQLContext(spark.sparkContext)
df =pd.read_csv('train_colabrative_matrix')

CheckCBF.check_CBF(spark,df, DataAccess.get_courses_df())

"""
#CheckClf.prepare_for_clustering(spark)
"""
os.remove("clf_check_10_.csv")
os.remove("clf_results_10_clusters.csv")
os.remove("clusters_10.csv")
"""

"""

num_fo_clusters = 10

for top in range(3, 15):
    num_of_courses = "2 to {}".format(top)

    os.remove("train_colabrative_matrix_without_one_course.csv")
    df = pd.read_csv('train.csv')
    df = CheckClf.get_users_that_took_more_then_n_courses(df, 2, top)
    df = CheckClf.create_train_colabrative_matrix(df, 'without_one_course')
    print("k")
    CheckClf.check_clf(spark, [2, 2, 2, 0, 0], num_fo_clusters, 'without_one_course', num_of_courses)
    print("f")
    os.remove("clf_check_{}_.csv".format(num_fo_clusters))
    os.remove("clf_results_{}_clusters.csv".format(num_fo_clusters))
    os.remove("clusters_{}.csv".format(num_fo_clusters))
  
    CheckClf.check_clf(spark, [10, 2, 2, 0, 0], num_fo_clusters, 'without_one_course', num_of_courses)

    os.remove("clf_check_100_.csv")
    os.remove("clf_results_100_clusters.csv")
    os.remove("clusters_100.csv")

    CheckClf.check_clf(spark, [2, 10, 2, 0, 0], num_fo_clusters, 'without_one_course', num_of_courses)

    os.remove("clf_check_100_.csv")
    os.remove("clf_results_100_clusters.csv")
    os.remove("clusters_100.csv")

    CheckClf.check_clf(spark, [2, 2, 10, 0, 0], num_fo_clusters, 'without_one_course', num_of_courses)

    os.remove("clf_check_100_.csv")
    os.remove("clf_results_100_clusters.csv")
    os.remove("clusters_100.csv")

    CheckClf.check_clf(spark, [2, 2, 2, 10, 10], num_fo_clusters, 'without_one_course', num_of_courses)

    os.remove("clf_check_100_.csv")
    os.remove("clf_results_100_clusters.csv")
    os.remove("clusters_100.csv")
    """

"""
df = pd.read_csv('train.csv')
df = CheckClf.get_users_that_took_more_then_n_courses(df, 2)
df = CheckClf.create_train_colabrative_matrix(df, 'without_one_course')
"""



# get initial demographic data from DataBase, prepare for clustering and saving to file
"""
# get demographic data
df = get_all_student_course_df(spark)

# prepare data for clustering
df = Cluster.preapare_for_clustering(df)
df.select("*").toPandas().to_csv('for_clustering.csv', index = False, header=True)
"""


# read clustering data from csv file, put waits and do clustering
"""
df = spark.read.csv("for_clustering.csv", header=True, inferSchema=True)
df = Data.put_waits(df, 2, 2, 2)
df = Cluster.generate_clusters(df, 50, spark)
df.show()
"""


# get data from DataBase and distribute to train val and test csv files
"""
# df = get_all_course_id_student_id_panda()
# Data.train_val_test(df)
"""


# create train colabrative matrix from train csv file
"""
df = pd.read_csv('train.csv')
print(df)
df = DataMatrix.get_data_matrix(df)
print(df)
df.to_csv('train_colabrative_matrix')
"""


# create clusters from csv file
"""
clusters = spark.read.csv("for_clustering.csv", header=True, inferSchema=True)
clusters = Data.put_waits(clusters, 2, 2, 2)
clusters = Cluster.generate_clusters(clusters, 1500, spark)
clusters.select("*").toPandas().to_csv('clusters_1500.csv')
"""

# show one cluster
"""
df = pd.read_csv('train_colabrative_matrix', index_col=0)
users_list = df.index.unique()

clusters = pd.read_csv('clusters_1000.csv')
f=open("CLF_Resoults.txt", "a+")

cluster = Cluster.fetch_cluster_by_cluster_number(998, clusters)
combine_list = list(set(users_list).intersection(cluster.userid_DI.unique()))
df1 = df[df.index.isin(combine_list)]
print(len(combine_list))
for user in combine_list:
    print( CF.get_curses(df1, user))
"""

# write CLF results to text file
"""
df = pd.read_csv('train_colabrative_matrix', index_col=0)
users_list = df.index.unique()

clusters = pd.read_csv('clusters_2000.csv')
f=open("CLF_Resoults_2000.txt", "a+")

for i in range(2000):
    f.write("cluster number: %d\n" % i)
    cluster = Cluster.fetch_cluster_by_cluster_number(i, clusters)

    combine_list = list(set(users_list).intersection(cluster.userid_DI.unique()))

    f.write("%d\n" % len(combine_list))

    df1 = df[df.index.isin(combine_list)]

    for user in combine_list:
        f.write("{} : {}\n".format(user, CF.get_curses(df1, user)))

f.close()
"""

# write CLF results to CSV file
"""
def append_list_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)


df = pd.read_csv('train_colabrative_matrix', index_col=0)
users_list = df.index.unique()

clusters = pd.read_csv('clusters_1000.csv')

for i in range(1000):
    cluster = Cluster.fetch_cluster_by_cluster_number(i, clusters)

    combine_list = list(set(users_list).intersection(cluster.userid_DI.unique()))

    df1 = df[df.index.isin(combine_list)]

    for user in combine_list:
        # Append a list as new line to an old csv file
        append_list_as_row('clf_results_1000_clusters.csv', [i, len(combine_list), user, CF.get_curses(df1, user)])
"""


# check clf results
"""
def append_list_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)


header_list = ["cluster", "cluster_size", "user", "clf_courses"]
df = pd.read_csv("clf_results_1000_clusters.csv", names=header_list)
df_list = df['user'].unique()

val = pd.read_csv('val.csv')
val_list = val['userid_DI'].unique()

userslist = list(set(val_list).intersection(df_list))

for user in userslist:
    i = 0
    k = 0
    val_courses = val.loc[val['userid_DI'] == user, 'course_id'].unique()
    clf_courses = df.loc[df['user'] == user, 'clf_courses'].iloc[0]
    for course in val_courses:
        if course in clf_courses:
            i += 1
        else:
            k += 1
    append_list_as_row('clf_check_1000_.csv', [user, i, k])
"""
"""
header_list = ["user", "yes", "no"]
df = pd.read_csv("clf_check_1000_.csv", names=header_list)

yes = df['yes'].sum()
no = df['no'].sum()

print(yes/(yes+no))

"""








