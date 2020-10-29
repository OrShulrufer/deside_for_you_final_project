


MY_SQL_CONNECTOR_PATH = "jars/mysql-connector-java-5.1.48.jar"
SPARK_LOG_CONF = 'true'
SPARK_EXECUTOR_MEMORY = '4g'
DB_HOST="212.80.207.89"
DB_NAME="DecideForYou"
DB_USER = "root"
DB_PASSWORD = "ZSMUnFWc3bZSMUnFWc3b"

from sqlalchemy import create_engine

DB_URL="{host}:3306/{dbname}".format(host=DB_HOST,dbname=DB_NAME)
pymysql_engine_url = "mysql+pymysql://{dbuser}:{dbpass}@{dburl}".format(dbuser=DB_USER, dbpass=DB_PASSWORD, dburl=DB_URL)
temp_table = "temp_table"
predictions_table = "predictions"
users_table="users"
student_details_view = 'all_students_details'
student_details_tbl='student_details'
driver = "com.mysql.jdbc.Driver"
student_course_table='student_course'
email_col='email'
password_col='password'
user_name_col='user_name'
google_id_col='google_id'
country_id_col='country_id'
YoB_unnormalized_col='YoB_NotNorm'
sp_updateLongLat='sp_updateLongLat'
sp_updateYOB='sp_updateYoB'
courses_with_topics_view='all_courses_with_topics'
courses_tbl="courses";
course_details_tbl='course_details'
general_update_status_col='general_update_status'


def update_clusters_by_df(predictions_df, spark):
    occurrence_num = "rn"
    df_query_str="select {userid},{prediction} as {cluster} from " \
              "(select userid_DI,prediction,row_number() OVER (PARTITION BY userid_DI ORDER BY userid_DI)  AS `{rn}` " \
              "from {temp_table} " \
              "group by userid_DI,prediction) t where {rn}=1".format(userid=columns_names.user_id,
                                                                     prediction=columns_names.prediction,
                                                                     cluster=columns_names.cluster,
                                                                rn=occurrence_num, temp_table=temp_table)
    query_str = "CALL sp_insert_predictions_from_temp()"
    predictions_df.registerTempTable(temp_table)
    exec_query_panda(query_str, spark.sql(df_query_str).toPandas())

def exec_query_panda(query, data_panda_DF=None):
    engine = create_engine(pymysql_engine_url)
    if data_panda_DF is not None:
        data_panda_DF.to_sql(temp_table, engine, if_exists='replace', index=False)
    if query is not None:
        with engine.begin() as conn:
            conn.execute(query)

def get_users_demogr_details_df(spark,and_userid=None,update_status=1):
    extend_where=""
    if and_userid is not None:
        extend_where="and ({update_status_col}=1 or {userid_col}='{userid}')".\
            format(update_status_col=general_update_status_col,userid_col=columns_names.user_id,userid=and_userid)
    elif update_status is not None:
        extend_where="and ({update_status_col}={update_status} and {clustre_col} is not null)".\
            format(update_status_col=general_update_status_col,update_status=update_status,clustre_col=columns_names.cluster)

    query_str="(SELECT {user_id},{education},{gender},{YOB},{long},{lat} " \
              "FROM {view} where 1=1 {extend_where}) as temp".\
        format(user_id=columns_names.user_id, education=columns_names.education, gender=columns_names.gender, YOB=columns_names.age,
               country=columns_names.country, long=columns_names.longitude,lat=columns_names.latitude, view=student_details_view,
               extend_where=extend_where)
    return fetch_df_from_large_sql_data(spark, query_str, columns_names.age, 8)

def fetch_df_from_large_sql_data(spark,push_down_query,partition_column,num_partitions=1,lower_bound=0,upper_bound=1):
    dataframe_mysql = spark.read.format("jdbc").option("url", "jdbc:mysql://"+DB_URL).option("driver", driver).\
    option("dbtable", push_down_query).option("user", DB_USER).option("password", DB_PASSWORD).\
    option("numPartitions", num_partitions).option("lowerBound", lower_bound).option("upperBound", upper_bound).option("partitionColumn", partition_column).load()
    #set numPartitions to 70 when student_course?
    return dataframe_mysql
