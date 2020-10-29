import BL.columns_names as columns_names


class CF:

    def get_curses(self, df, user_id, spark):
        # after returning result and done using the temp table => sqlContext.dropTempTable("temp_table_name")
        df.registerTempTable("students_clusters")
        cluster_by_user = spark.sql("SELECT * "
                                    "FROM students_clusters "
                                    "WHERE prediction = "
                                    "(SELECT DISTINCT prediction "
                                    "FROM students_clusters "
                                    "WHERE userid_DI = '{}')".
                                    format(user_id))

        cluster_by_user.registerTempTable("selected_cluster")
        curses_that_user_tooke = spark.sql("SELECT DISTINCT course_id "
                                           "FROM selected_cluster "
                                           "WHERE userid_DI = '{}'".
                                           format(user_id))

        curses_that_user_tooke.registerTempTable("curses_that_user_tooke")
        users_that_tooke_the_same_classes = spark.sql("SELECT DISTINCT s.userid_DI "
                                                      "FROM selected_cluster as s, curses_that_user_tooke  as c "
                                                      "WHERE s.course_id = "
                                                      "c.course_id "
                                                      "and "
                                                      "s.userid_DI != '{}'".
                                                      format(user_id))

        users_that_tooke_the_same_classes.registerTempTable("users_that_tooke_the_same_classes")
        return spark.sql("SELECT DISTINCT s.course_id "
                         "FROM selected_cluster as s inner join "
                         "users_that_tooke_the_same_classes as u ON s.userid_DI = u.userid_DI  "
                         "WHERE  "
                         "s.course_id NOT IN (select course_id from curses_that_user_tooke) ").\
                         toPandas()[columns_names.course_id].to_list()


