from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import datetime
import BL.columns_names as columns_names


class Data:

    def prepare_data(self, df, spark):

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
        df_selected_columns = self.normalize(df_selected_columns, columns_names.age)

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
        countries_students_join = self.normalize(countries_students_join, columns_names.latitude)

        # normalize longitude column
        countries_students_join = self.normalize(countries_students_join, columns_names.longitude)



        return countries_students_join



    def put_waits(self, df, w0=1, w1=1, w2=1, w3=1, w4=1):

        df = df.withColumn(columns_names.gender, df[columns_names.gender] * w0)
        df = df.withColumn(columns_names.age, df[columns_names.age] * w1)
        df = df.withColumn(columns_names.education, df[columns_names.education] * w2)
        df = df.withColumn(columns_names.latitude, df[columns_names.latitude] * w3)
        df = df.withColumn(columns_names.longitude, df[columns_names.longitude] * w4)

        return df

    def normalize(self, df, column_name):
        # get minimum and maximum ages
        max = df.select(column_name).rdd.max()[0]
        min = df.select(column_name).rdd.min()[0]

        # normalize age column
        return df.withColumn(column_name, (df[column_name]-min)/(max-min))



