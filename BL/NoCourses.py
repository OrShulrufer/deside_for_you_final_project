from pyspark.sql import functions as F


class NoCourses:

    def get_courses(self, df_courses, topic, spark):
        # register data frame as table for using SQL
        df_courses.registerTempTable("courses")

        # get all user's courses from the past
        df_courses_with_topic = spark.sql("SELECT * "
                                             "FROM courses "
                                             "WHERE Topics = '{}'".
                                             format(topic))

        return df_courses_with_topic.orderBy(F.desc("% Certified of > 50% Course Content Accessed"))\
                     .toPandas()["course_number"].values[:10]


