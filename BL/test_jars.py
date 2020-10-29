from pyspark.sql import SparkSession,HiveContext
import BL.columns_names as columns_names
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.conf import SparkConf
import matplotlib.pyplot as plt

conf = SparkConf()
conf.set('spark.logConf', 'true')
conf.set("spark.jars", "jars/mysql-connector-java-5.1.48.jar")

# Create Spark Session
spark = SparkSession.builder.appName("SQLBasicResultexample").config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

dataframe_mysql = spark.read.format("jdbc").option("url", "jdbc:mysql://remotemysql.com:3306/kWlDFYYcCh").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "students_courses").option("user", "kWlDFYYcCh").option("password", "BMhM2NUKbc").load()
#spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/faceafeka").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "friends").option("user", "root").option("password", "").load()
dataframe_mysql.show()
# read demographic data
demographic_df = spark.read.format("libsvm").csv("data/demographic.csv", inferSchema="true", header="true")

# assemble feathers vector
vecAssembler = VectorAssembler(
    inputCols=[columns_names.gender, columns_names.education, columns_names.age, columns_names.longitude, columns_names.latitude],
    outputCol= "features")
demographic_df = vecAssembler.transform(demographic_df)

# Trains a k-means model.
kmeans = KMeans().setK(5).setInitMode("k-means||").setSeed(1).setFeaturesCol("features")

#print(kmeans.explainParams())

model = kmeans.fit(demographic_df)

# Make predictions
predictions = model.transform(demographic_df)
predictions.show()

predictions = predictions.drop(predictions["features"])
predictions.show()
#predictions = predictions.toPandas()

predictions.coalesce(1).write.option("header", "true").option("inferSchema", "true").csv("data/5_clusters.csv")