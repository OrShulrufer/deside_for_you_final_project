
from flask import Flask, render_template,request, send_from_directory, jsonify

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


app = Flask(__name__, static_url_path='')
# from UserController import validate_id_token #currently not working in Or's pc
from UserController import check_user,insert_and_get_user,get_the_user
app.config.from_pyfile('config.py')  # debug=True cause to restart this file several times

print("Starting setup Spark")

conf = SparkConf()
conf.set('spark.logConf', app.config["SPARK_LOG_CONF"])
conf.set("spark.jars", app.config["MY_SQL_CONNECTOR_PATH"])
conf.set("spark.executor.memory ", app.config["SPARK_EXECUTOR_MEMORY"])     # wont cause problems?

# Create Spark Session
spark = SparkSession.builder.appName("DecideForYou").config(conf=conf).master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

