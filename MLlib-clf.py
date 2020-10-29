from __future__ import print_function
import pandas as pd
from flask import Flask
from DataAccess import get_all_student_course_df
from DataAccess import get_all_course_id_student_id_panda
from DataAccess import get_all_course_id_student_id

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
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
import re
from pyspark.sql import SparkSession


from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row



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

"""
df = pd.read_csv('train.csv')
print(df)
num_courses = df.groupby(df["course_id"]).count()
num_courses = num_courses.sort_values(by='userid_DI', ascending=False)
print(num_courses)

def label_race (row):
   if row['course_id'] == "CS50x":
      return 10
   if row['course_id'] == "6.00x":
      return 8
   if row['course_id'] == "6.002x":
      return 6
   if row['course_id'] == "ER22x":
      return 4
   if row['course_id'] == "PH207x":
      return 2
   if row['course_id'] == "PH278x":
      return 1
   if row['course_id'] == "8.02x":
      return 1
   if row['course_id'] == "CB22x":
      return 1
   if row['course_id'] == "14.73x":
      return 1
   if row['course_id'] == "7.00x":
      return 1
   if row['course_id'] == "3.091x":
      return 1
   if row['course_id'] == "8.MReV":
      return 1
   if row['course_id'] == "2.01x":
      return 1
   return 1



#  CS50x,ER22x,PH278x,CB22x,PH207x,6.002x,2.01x,3.091x,6.00x,7.00x,8.02x,14.73x,8.MReV


df = pd.read_csv('train.csv')
df["rating"] = df.apply (lambda row: label_race(row), axis=1)


gender = {'CS50x': 1,'ER22x':2,'PH278x':3,'CB22x':4,'PH207x':5,'6.002x':6,'2.01x':7,'3.091x':8,'6.00x':9,
          '7.00x':10,'8.02x':11,'14.73x':12,'8.MReV':13}

df["course_num"]= [gender[item] for item in df["course_id"]]
print(df)

print(df)
list_of_users = df["userid_DI"].unique()
print(list_of_users)
df1 = pd.DataFrame(list_of_users, columns = ['user'])
df1["user_num"] = df1.index
print(df1)
df = df.join(df1.set_index('user'), on='userid_DI')
print(df)
df.to_csv("train_clf.csv")
"""
sqlCtx = SQLContext(spark.sparkContext)
df = sqlCtx.createDataFrame(pd.read_csv("train_clf.csv"))

import sys
if sys.version >= '3':
    long = int


if __name__ == "__main__":
    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
   # als = ALS(maxIter=10, regParam=0.01, userCol="user_num", itemCol="course_num", ratingCol="rating",
    #          coldStartStrategy="drop")
    als = ALS(maxIter=10, regParam=0.01, userCol="user_num", itemCol="course_num",
                        coldStartStrategy="drop")
    model = als.fit(df)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(df)
    model.

    predictions.sow()
   # predictions.toPandas().sort_values(by='user_num', ascending=True).to_csv("predictions.csv")

"""
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    # Generate top 10 movie recommendations for each user
    userRecs = model.recommendForAllUsers(5)
    # Generate top 10 user recommendations for each movie
    movieRecs = model.recommendForAllItems(5)

    # Generate top 10 movie recommendations for a specified set of users
    users = df.select(als.getUserCol()).distinct().limit(3)
    userSubsetRecs = model.recommendForUserSubset(users, 5)
    # Generate top 10 user recommendations for a specified set of movies
    movies = df.select(als.getItemCol()).distinct().limit(3)
    movieSubSetRecs = model.recommendForItemSubset(movies, 5)

    userRecs.show()
    userRecs.toPandas().to_csv("userRecs.csv")

    #movieRecs.toPandas().to_csv("movieRecs.csv")

    #userSubsetRecs.toPandas().to_csv("userSubsetRecs.csv")

    # movieSubSetRecs.toPandas().to_csv("movieSubSetRecs.csv")

    spark.stop()



pat = re.compile(r"Row\(course_num=(\w+)")


df = pd.read_csv("userRecs.csv", index_col= "Unnamed: 0")
print(df)
df['recomendations'] = df["recommendations"].map(lambda x: pat.findall(x))
df = df.sort_values(by='user_num', ascending=True)
print(df)


df2 = pd.read_csv('train_clf.csv',index_col= "Unnamed: 0").drop_duplicates('userid_DI', keep='first')
df2 = df2.drop(columns=['course_id', 'rating','course_num'])
print(df2)

df = df2.join(df.set_index('user_num'), on='user_num').drop(columns=["recommendations"])
print(df)
df.to_csv('clf_predictions_MLlib.csv')


def append_list_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)


courses = {"1": 'CS50x', "2":'ER22x', "3":'CB22x', "4":'CB22x', "5":'PH207x',"6":'6.002x', "7":'2.01x',
           "8":'3.091x', "9":'6.00x', "10":'7.00x',"11":'8.02x', "12":'14.73x', "13":'8.MReV'}

df = pd.read_csv("clf_predictions_MLlib.csv", index_col= "Unnamed: 0")
print(df)

val_df = pd.read_csv("val.csv")
val_list =val_df["userid_DI"].unique()
users = df["userid_DI"].unique()
userslist = list(set(val_list).intersection(users))

for user in userslist:
    x = 0
    y = 0
    val_courses = val_df.loc[val_df['userid_DI'] == user, 'course_id'].unique()
    clf_courses = df.loc[df['userid_DI'] == user, 'recomendations'].iloc[0]
    for j in val_courses:
        yes = False
        for i in range(5):
            course = courses.get(clf_courses[i])
            if course is not None:
               if course == j:
                   yes = True
        if yes is True:
            x =+ 1
        else:
            y +=1

    append_list_as_row("Mlib_check.csv",[user,x,y])


header_list = ["user", "yes", "no"]
df = pd.read_csv("Mlib_check.csv", names=header_list)
yes = df['yes'].sum()
no = df['no'].sum()
presantage = (yes / (yes + no)) *100
print(presantage)





"""


