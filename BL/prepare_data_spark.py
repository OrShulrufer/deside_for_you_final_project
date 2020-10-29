from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

import datetime
import BL.columns_names as columns_names


class Data:
    @staticmethod
    def prepare_data(df, spark):

        # Select needed columns
        df_selected_columns = df.select(columns_names.user_id,
                                                 columns_names.gender,
                                                 columns_names.age,
                                                 columns_names.education,
                                                 columns_names.country,
                                                 columns_names.course_id,
                                                 columns_names.grade)

        # convert nulls in grade column to 0
        df_selected_columns = df_selected_columns.withColumn(
            columns_names.grade, F.when(F.col(columns_names.grade).isNull(), 0.0).
            otherwise(F.col(columns_names.grade)))

        # convert long course id to only id by number
        df_selected_columns = df_selected_columns.withColumn(
            columns_names.course_id, F.split(df_selected_columns[columns_names.course_id], '/')[1])

        # changing "gender" column (male=o, female=1, nan=0.5)
        df_selected_columns = df_selected_columns.withColumn(
            columns_names.gender,
            F.when(F.col(columns_names.gender).isNull(), 0.5).
            otherwise(F.when(F.col(columns_names.gender) == "m", 0).
            otherwise(F.when(F.col(columns_names.gender) == "f", 1))))

        current_year = datetime.datetime.now().year

        # update from  year of birth to age
        df_selected_columns = df_selected_columns.withColumn(
            columns_names.age, F.when(F.col(columns_names.age) == "NA", 30).
            otherwise(F.when(F.col(columns_names.age).isNull(), 30).
            otherwise(current_year - F.col(columns_names.age))))
        """
        # get minimum and maximum ages
        max_age = df_selected_columns.select(columns_names.age).rdd.max()[0]
        min_age = df_selected_columns.select(columns_names.age).rdd.min()[0]

        # normalize age column
        df_selected_columns = df_selected_columns.withColumn(
            columns_names.age, (df_selected_columns[columns_names.age]-min_age)/(max_age-min_age))
        """
        # normalize age column
        df_selected_columns = Data.normalize(df_selected_columns, columns_names.age)

        # normalize education column
        df_selected_columns = df_selected_columns.withColumn(
            columns_names.education, F.when(F.col(columns_names.education).isNull(), 0.0).
            otherwise(F.when(F.col(columns_names.education) == 'Less than Secondary', 0.0).
            otherwise(F.when(F.col(columns_names.education) == 'Secondary', 0.25).
            otherwise(F.when(F.col(columns_names.education) == 'Bachelor\'s', 0.5).
            otherwise(F.when(F.col(columns_names.education) == 'Master\'s', 0.75).
            otherwise(F.when(F.col(columns_names.education) == 'Doctorate', 1.0)))))))

        # read countries data
        countries_df = spark.read.csv("data/countries.csv", inferSchema="true", header="true")

        # joining two data sets: countries and students
        countries_students_join = df_selected_columns.join(
            countries_df, df_selected_columns[columns_names.country] == countries_df.name, how='left')

        # dropping unused columns
        countries_students_join = countries_students_join.drop("name").drop(columns_names.country).drop("country")

        # if null then like US latitude
        countries_students_join = countries_students_join.withColumn(
            columns_names.latitude, F.when(F.col(columns_names.latitude).isNull(),  37.09024).
            otherwise(F.col(columns_names.latitude)))

        # if null then like US longitude
        countries_students_join = countries_students_join.withColumn(
            columns_names.longitude, F.when(F.col(columns_names.longitude).isNull(),  -95.712891).
            otherwise(F.col(columns_names.latitude)))

        # normalize latitude column
        countries_students_join = Data.normalize(countries_students_join, columns_names.latitude)

        # normalize longitude column
        countries_students_join = Data.normalize(countries_students_join, columns_names.longitude)



        return countries_students_join

    @staticmethod
    def put_waits(df, w0=1, w1=1, w2=1, w3=1, w4=1):
        df[columns_names.gender] = df[columns_names.gender] * w0
        df[columns_names.age] =  df[columns_names.age] * w1
        df[columns_names.education] = df[columns_names.education] * w2
        df[columns_names.latitude] =  df[columns_names.latitude] * w3
        df[columns_names.longitude] = df[columns_names.longitude] * w4

        return df

    @staticmethod
    def normalize(df, column_name):
        # get minimum and maximum ages
        max = df.select(column_name).rdd.max()[0]
        min = df.select(column_name).rdd.min()[0]

        # normalize age column
        return df.withColumn(column_name, (df[column_name]-min)/(max-min))

    @staticmethod
    def train_val_test(data):
        data = data[['userid_DI', 'course_id']]
        test = data.iloc[::5, :]
        test.to_csv("test.csv", index=False)

        val = data.iloc[2::5, :]
        val.to_csv("val.csv", index=False)

        train1 = data.iloc[1::5, :]
        train3 = data.iloc[3::5, :]
        train4 = data.iloc[4::5, :]
        train = pd.concat([train1, train3, train4]).sort_index().reset_index(drop=True)
        train.to_csv("train.csv", index=False)




