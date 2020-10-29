import BL.columns_names as columns_names
from DataAccess import update_clusters_by_df,update_user_cluster, get_users_demogr_details_df
from DataAccess import get_demogr_details_by_user
from BL.prepare_data_spark import Data
from sklearn.cluster import KMeans
import pandas
import numpy as np
import pandas as pd
import time



class Cluster:
    @staticmethod
    def rearrange_clusters(num_of_clusters=10, waits=[10, 1, 10, 1, 10]):
        print("0")
        demograpic_data = get_users_demogr_details_df()
        print("1")
        demograpic_data = Data.put_waits(demograpic_data, waits[0], waits[1], waits[2], waits[3], waits[4])
        print("2")
        predictions_df, clustering = Cluster.generate_clusters(demograpic_data, num_of_clusters)
        return predictions_df, clustering

    @staticmethod
    def update_db_clusters(predictions_df):
            update_clusters_by_df(predictions_df)

    @staticmethod
    def update_user_cluster(prediction, userid):
        update_user_cluster(userid, prediction)


    @staticmethod
    def generate_clusters(demograpic_data, num_of_clusters):
        features = demograpic_data[[columns_names.gender,
                                    columns_names.education,
                                    columns_names.age,
                                    columns_names.longitude,
                                    columns_names.latitude]]
        clustering = KMeans(num_of_clusters, random_state=0)
        labels = clustering.fit(features).labels_
        demograpic_data['predictions'] = labels
        return demograpic_data, clustering

    @staticmethod
    def predict_user_cluster(clustering, user_id):
        user_demografic_data = get_demogr_details_by_user(user_id)[0]
        return clustering.predict([[user_demografic_data.get(columns_names.gender),
                                   user_demografic_data.get(columns_names.education),
                                   user_demografic_data.get(columns_names.age),
                                   user_demografic_data.get(columns_names.longitude),
                                   user_demografic_data.get(columns_names.latitude)]])[0]


