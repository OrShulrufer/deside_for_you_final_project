import pandas as pd
from flask import Flask
from DataAccess import get_users_demogr_details_df
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
import BL.columns_names as columns_names
from pyspark.ml.clustering import KMeans
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.feature import VectorAssembler
from DataAccess import update_clusters_by_df
from BL.prepare_data_spark import Data
from DataAccess import get_users_demogr_details_df
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import seaborn as sbs
from matplotlib.ticker import MaxNLocator
import numpy as np
from sklearn.decomposition import PCA
import plotly.graph_objs as go
from plotly.offline import iplot

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

df_students = get_users_demogr_details_df(spark)
df_students = Data.put_waits(df_students, 200, 200, 200, 100, 100)
df_students.toPandas().to_csv("students_demographic_data.csv")



# assemble feathers vector
vec_assembler = VectorAssembler(
    inputCols=[columns_names.gender,
               columns_names.education,
               columns_names.age,
               columns_names.longitude,
               columns_names.latitude],
    outputCol="features")

df = vec_assembler.setHandleInvalid("skip").transform(df_students)



def elbow_cheak(max_clusters):
    cost = np.zeros(max_clusters)
    for k in range(2, max_clusters):
        kmeans = KMeans()\
                .setK(k)\
                .setSeed(1) \
                .setFeaturesCol("features")\
                .setPredictionCol("cluster")

        model = kmeans.fit(df)
        cost[k] = model.computeCost(df) # requires Spark 2.0 or later


    fig, ax = plt.subplots(1,1, figsize =(8,6))
    ax.plot(range(2,max_clusters),cost[2:max_clusters])
    ax.set_xlabel('k')
    ax.set_ylabel('cost')
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    plt.show()



kmeans = KMeans()\
            .setK(10)\
            .setSeed(1) \
            .setFeaturesCol("features")\
            .setPredictionCol("cluster")

model = kmeans.fit(df)
predictions = model.transform(df)

centroids = model.clusterCenters()

df = predictions.toPandas()
cols = [columns_names.gender,
               columns_names.education,
               columns_names.age,
               columns_names.longitude,
               columns_names.latitude]

pca = PCA(n_components = 3)
df['X'] = pca.fit_transform(df[cols])[:,0]
df['Y'] = pca.fit_transform(df[cols])[:,1]
df['Z'] = pca.fit_transform(df[cols])[:,2]
df = df.reset_index()
print(df.tail())
df.to_csv("for_graph_3d.csv")
"""
trace0 = go.Scatter(x = df[df["cluster"] == 0]["X"],y = df[df["cluster"] == 0]["Y"],
                    name = "Cluster 1",
                    mode = "markers",
                    marker = dict(size = 10,
                                  color = "rgba(15,152,152,0.5)",
                                  line = dict(width =1, color = "rgb(0,0,0)")))

trace1 = go.Scatter(x = df[df["cluster"] == 1]["X"],y = df[df["cluster"] == 1]["Y"],
                    name = "Cluster 2",
                    mode = "markers",
                    marker = dict(size = 10,
                                  color = "rgba(180,18,180,0.5)",
                                  line = dict(width =1, color = "rgb(0,0,0)")))

trace2 = go.Scatter(x = df[df["cluster"] == 2]["X"],y = df[df["cluster"] == 2]["Y"],
                    name = "Cluster 3",
                    mode = "markers",
                    marker = dict(size = 10,
                                  color = "rgba(132,132,132,0.5)",
                                  line = dict(width =1, color = "rgb(0,0,0)")))

data = [trace0, trace1, trace2]


iplot(dada)
"""

import pandas as pd
import plotly.graph_objs as go
from plotly.offline import init_notebook_mode, iplot
init_notebook_mode()
df =pd.read_csv("for_graph_3d.csv")
