from BL.create_clusters import Cluster
from BL.prepare_data_spark import Data
from csv import writer
from DataAccess import get_users_demogr_details_df
import pandas as pd
from hynrid_fo_check import Hybrid
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk
import numpy as np

class CheckHyrid:

    def get_user_courses_recommendation(df_clusters, clf_matrix_df, df_courses, user_course_df, val_df):

        nltk.download('punkt')
        nltk.download('stopwords')
        stop_words = set(stopwords.words('english'))

        user_course_list = user_course_df['userid_DI'].unique()
        clusters_users = df_clusters['userid_DI'].unique()

        val_list = val_df['userid_DI'].unique()

        userslist = list(set(val_list).intersection(user_course_list))
        userslist = list(set(userslist).intersection(clusters_users))
        userslist.sort()
        #userslist = userslist[:1000]
        userslist = np.array_split(userslist, 5)


        l = list(map(lambda x:(x, Hybrid.get_courses(clf_matrix_df, df_clusters, df_courses, user_course_df, x, stop_words)), userslist[0]))
        print("end 1")
        l = list(map(lambda x: CheckHyrid.get_no_yes(x, val_df), l))
        print("end 2")

        sum_yes = sum(i[0] for i in l)
        print("end 3")
        sum_no = sum(i[1] for i in l)
        print("end 4")
        presentae = (sum_yes / (sum_yes + sum_no)) * 100
        return presentae
        #val_courses = val_df.loc[val_df['userid_DI'] == user, 'course_id'].unique()


    def create_clusters(spark, spark_df_student_demografic_data, waits=[10, 1, 10, 1, 10], num_of_clusters=10):
        spark_df_demographic_data = Data.put_waits(spark_df_student_demografic_data, waits[0], waits[1], waits[2], waits[3], waits[4])
        clusters = Cluster.generate_clusters(spark_df_demographic_data, num_of_clusters, spark)
        #clusters.select("*").toPandas().to_csv('clusters_{}.csv'.format(num_of_clusters))
        return clusters.toPandas()

    def get_users_that_took_more_then_n_courses(df, number_of_courses, top=100):
        df = df[df['userid_DI'].groupby(df['userid_DI']).transform('size') >= number_of_courses]
        df = df[df['userid_DI'].groupby(df['userid_DI']).transform('size') <= top]
        return df

    def append_list_as_row(file_name, list_of_elem):
        # Open file in append mode
        with open(file_name, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            csv_writer = writer(write_obj)
            # Add contents of list as last row in the csv file
            csv_writer.writerow(list_of_elem)

    def get_train_val_test_data(df):

        train1 = df.iloc[1::10, :]
        train2 = df.iloc[2::10, :]
        train3 = df.iloc[3::10, :]
        train4 = df.iloc[4::10, :]
        train6 = df.iloc[6::10, :]
        train7 = df.iloc[7::10, :]
        train8 = df.iloc[8::10, :]
        train9 = df.iloc[9::10, :]
        train = pd.concat([train1, train2, train3, train4, train6, train7, train8, train9]).sort_index().reset_index(drop=True)
        train.to_csv('train_students_course.csv', index=False, header=True)

        val = df.iloc[5::10, :]
        val.to_csv('val_students_course.csv', index=False, header=True)

        test = df.iloc[10::10, :]
        test.to_csv('test_students_course.csv', index=False, header=True)

    def get_no_yes(user_cours_tuple, val_df):
        yes = 0
        no = 0
        t = ()
        val_courses = val_df.loc[val_df['userid_DI'] == user_cours_tuple[0], 'course_id'].unique()
        for i in val_courses:
            if i in user_cours_tuple[1]:
                yes += 1
            else:
                no += 1
        t = (yes, no)
        return t

    def get_percentage_of_success(filename):
        header_list = ["user","courses", "yes", "no"]
        df = pd.read_csv("{}".format(filename), names=header_list, index_col=False)
        print(df)

        yes = df['yes'].sum()
        no = df['no'].sum()

        print(len(df['yes'].tolist()))

        return(yes / (yes + no)) * 100





