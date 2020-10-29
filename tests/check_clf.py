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
from ast import literal_eval



class CheckClf:


    def check_clf(spark, waits, num_of_clusters, add_to_name_of_matrix, num_courses):
        CheckClf.create_clusters(spark, waits, num_of_clusters)
        CheckClf.write_CLF_results(num_of_clusters, add_to_name_of_matrix, num_of_clusters)
        CheckClf.check_clf_results(num_of_clusters)
        CheckClf.get_percentage_of_success(num_of_clusters, waits, num_courses)



    def prepare_for_clustering(spark):
        # get demographic data
        df = get_users_demogr_details_df(spark)

        df = df.select("*").toPandas()

        train1 = df.iloc[1::5, :]
        train3 = df.iloc[3::5, :]
        train4 = df.iloc[4::5, :]
        train = pd.concat([train1, train3, train4]).sort_index().reset_index(drop=True)

        # prepare data for clustering
        train = Cluster.preapare_for_clustering(df)
        train.to_csv('for_clustering.csv', index=False, header=True)

    def distribute_to_train_val_and_test(self):
        df = get_all_course_id_student_id_panda()
        Data.train_val_test(df)


    def create_train_colabrative_matrix(df, add_to_name_of_matrix):
        df = DataMatrix.get_data_matrix(df)
        df.to_csv('train_colabrative_matrix_{}.csv'.format(add_to_name_of_matrix))


    def create_clusters(spark, waits, num_of_clusters):
        clusters = spark.read.csv("for_clustering.csv", header=True, inferSchema=True)
        clusters = Data.put_waits(clusters, waits[0], waits[1], waits[2], waits[3], waits[4])
        clusters = Cluster.generate_clusters(clusters, num_of_clusters, spark)
        clusters.select("*").toPandas().to_csv('clusters_{}.csv'.format(num_of_clusters))


    def write_CLF_results(cluster_size, add_to_name_of_matrix, num_of_clusters):
        df = pd.read_csv('train_colabrative_matrix_{}.csv'.format(add_to_name_of_matrix), index_col=0)
        users_list = df.index.unique()

        clusters = pd.read_csv('clusters_{}.csv'.format(cluster_size))

        for i in range(num_of_clusters):
            cluster = Cluster.fetch_cluster_by_cluster_number(i, clusters)

            combine_list = list(set(users_list).intersection(cluster.userid_DI.unique()))

            df1 = df[df.index.isin(combine_list)]

            for user in combine_list:
                # Append a list as new line to an old csv file
                CheckClf.append_list_as_row('clf_results_{}_clusters.csv'.format(cluster_size),
                                   [i, len(combine_list), user, CF.get_curses(df1, user)])


    def append_list_as_row(file_name, list_of_elem):
        # Open file in append mode
        with open(file_name, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            csv_writer = writer(write_obj)
            # Add contents of list as last row in the csv file
            csv_writer.writerow(list_of_elem)


    def check_clf_results(num_of_clusters):
        header_list = ["cluster", "cluster_size", "user", "clf_courses"]
        df = pd.read_csv("clf_results_{}_clusters.csv".format(num_of_clusters),
                         names=header_list,
                         converters={"clf_courses": lambda x: x.strip("[]").replace("'","").split(", ")})

        df_list = df['user'].unique()

        val = pd.read_csv('val.csv')
        val_list = val['userid_DI'].unique()

        userslist = list(set(val_list).intersection(df_list))

        for user in userslist:
            i = 0
            k = 0
            val_courses = val.loc[val['userid_DI'] == user, 'course_id'].unique()
            clf_courses = df.loc[df['user'] == user, 'clf_courses'].iloc[0]

            if any(map(str.isdigit, clf_courses[0])):
                length = len(clf_courses)
            else:
                length = 0


            for course in val_courses:
                if course in clf_courses:
                    i += 1
                else:
                    k += 1

            CheckClf.append_list_as_row('clf_check_{}_.csv'.format(num_of_clusters), [user, i, k, length])

    def get_percentage_of_success(num_of_clusters, waits, num_courses):
        header_list = ["user", "yes", "no", "num_of_courses_from_clf"]
        df = pd.read_csv("clf_check_{}_.csv".format(num_of_clusters), names=header_list)

        yes = df['yes'].sum()
        no = df['no'].sum()

        presantage = (yes / (yes + no)) *100
        courses_took = df.groupby('num_of_courses_from_clf').size()
        courses_took = courses_took.to_frame().to_records().tolist()
        CheckClf.append_list_as_row('final_clf_check.csv', [num_of_clusters, waits, num_courses, courses_took, presantage])


    def get_users_that_took_more_then_n_courses(df, number_of_courses, top):
        df = df[df['userid_DI'].groupby(df['userid_DI']).transform('size') >= number_of_courses]
        df = df[df['userid_DI'].groupby(df['userid_DI']).transform('size') <= top]
        return df
