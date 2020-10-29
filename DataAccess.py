from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import mysql.connector
import MySQLdb.cursors
import pandas as pd
import pandasql as ps
from config import DB_NAME,DB_HOST, DB_USER,DB_PASSWORD
import BL.columns_names as columns_names

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
is_course_provider_col= 'is_course_provider'
country_id_col='country_id'
YoB_unnormalized_col='YoB_NotNorm'
sp_updateLongLat='sp_updateLongLat'
sp_updateYOB='sp_updateYoB'
courses_with_topics_view='all_courses_with_topics'
courses_tbl="courses"
course_details_tbl='course_details'
general_update_status_col='general_update_status'
web_link_col = 'web_link'

def insert_user(email, password, user_name='', google_id=''):
    query = "INSERT INTO {users} " \
            "SELECT * FROM (SELECT %s as a, %s as b, %s as c,'{googleid}' as d,0) AS tmp " \
            "WHERE NOT EXISTS (SELECT {email} FROM {users} WHERE {email}=%s) Limit 1;".\
        format(users=users_table, googleid=google_id,email=email_col)
    param_list = [email, password, user_name, email]
    return exec_non_query(query, None, tuple(param_list))


def insert_user_to_other_tbls(email):
    query1 = "INSERT INTO {predictions} (`{userid}`) " \
          "SELECT * FROM (select \"{email}\") AS tmp " \
            "WHERE NOT EXISTS (SELECT {userid} FROM {predictions} WHERE {userid}=\"{email}\") Limit 1;"\
        .format(predictions=predictions_table,userid=columns_names.user_id,email=email)
    query2 = "INSERT INTO {details} (`{userid}`) " \
          "SELECT * FROM (select \"{email}\") AS tmp " \
            "WHERE NOT EXISTS (SELECT {userid} FROM {details} WHERE {userid}=\"{email}\") Limit 1;"\
        .format(details=student_details_tbl,userid=columns_names.user_id,email=email)
    return exec_non_query(query1,query2)


def get_user(email,password=None):
    query = "select {users}.*,{gender}" \
            ",{country_id},{ed_col} as education,{YoB_NotNorm} as YOB, {is_provider} " \
                "from {users} left join {students} " \
                "on {userid}={email} where {email}=\"{email_value}\" ".\
            format(gender=columns_names.gender, country_id=country_id_col, ed_col=columns_names.education,
                   YoB_NotNorm=YoB_unnormalized_col, is_provider=is_course_provider_col, users=users_table, students="student_details",
                   userid=columns_names.user_id, email=email_col, email_value=email,password=password_col)
    if password is None:
        query += " limit 1"
        return exec_query(query)
    else:
        param_list = [password]
        query += " and {password}=%s limit 1".format(password=password_col)
        return exec_query(query,tuple(param_list))


def update_user(email,user_name,education,YoB,gender,country_id,update_demographic):
    # update all user's details
    query1 = "UPDATE {users} inner join {students} on {users}.{email_col}={students}.{userid}" \
            " SET {usernamecol} = %s," \
            " {educationcol}={education}," \
            " {YoB_NotNormcol}={YoB}," \
            " {gendercol}={gender}," \
            " {countryidcol}={countryid} " \
            " where {userid}=\"{email}\" and {email_col}=\"{email}\";".\
        format(users=users_table, students=student_details_tbl, email_col=email_col, userid=columns_names.user_id,
               usernamecol=user_name_col,  educationcol=columns_names.education, education=education,
               YoB_NotNormcol=YoB_unnormalized_col, YoB=YoB,
               gendercol=columns_names.gender, gender=gender, countryidcol=country_id_col, countryid=country_id, email=email)

    param_list = [user_name]
    query_result = exec_non_query(query1, None, tuple(param_list))

    # update all users yob and user's longlat
    if update_demographic:
        call_sp(sp_updateYOB, [YoB, email])
        call_sp(sp_updateLongLat, [email])

    return query_result


def get_user_courses(userid):
    query = "select {courseid} from {stdnt_crs} where {userid}=\"{userid_value}\"".\
        format(courseid=columns_names.course_id,stdnt_crs=student_course_table, userid=columns_names.user_id,userid_value=userid)
    return exec_query(query)


def update_user_courses(email, courses):
    values = ""
    i = 0
    for course in courses:
        i +=1
        values += "'{course_id}'".format(course_id=course)
        if i < len(courses):
            values += ","
    if len(courses) == 0: values = "''"
    query1 = "DELETE FROM {studentcourses} WHERE {userid}=\"{email}\" and {courseidcol} NOT IN ({VALUES}) ;".\
        format(studentcourses=student_course_table, userid=columns_names.user_id, email=email,
               courseidcol=columns_names.course_id, VALUES=values)
    values = ""
    i = 0
    for course in courses:
        i +=1
        values += "(\"{email}\",\"{course_id}\")".format(email=email,course_id=course)
        if i < len(courses):
            values += ","
    query2 = "INSERT INTO {studentcourses} " \
             "VALUES {VALUES} ON DUPLICATE KEY UPDATE {useridcol}=VALUES({useridcol}),{courseidcol}=VALUES({courseidcol})" \
        .format(studentcourses=student_course_table,VALUES=values,
                useridcol=columns_names.user_id, courseidcol=columns_names.course_id)

    if len(courses)>0:
        return exec_non_query(query1, query2)
    else:
        return exec_non_query(query1)


def delete_user(userid, delete_all_users=False):
    if userid is None and delete_all_users == True:
        userid='all'
    call_sp('sp_delete_user', [userid])


def get_topics():
    query = "select * from (select topic1 as topic from all_courses_with_topics union all " \
            "select topic2 from all_courses_with_topics union all " \
            "select topic3 from all_courses_with_topics) as tb " \
            "group by topic"
    return exec_query(query)

def get_courses(topic=None, to_list=False, spark=None, certified_course_num=0):
    query_str = "select * from all_courses_with_topics "
    add_to_query = ""
    if topic is not None:
        add_to_query += "where '{topic}' in (topic1,topic2,topic3) ".format(topic=topic)
    if certified_course_num is not 0:
        add_to_query += "order by certified_course_half_content desc limit {certified_course}"\
            .format(certified_course=certified_course_num)

    query_str = query_str+add_to_query
    if to_list == True:
        return exec_query(query_str)
    if spark is not None:
        return fetch_df_from_large_sql_data(spark, "({query}) as temp".format(query=query_str),
                                            columns_names.certified_course_half_content, 1,0,100)
    return  fetch_df_by_query(query_str)


def get_courses_details():
    query = "select * from {cd} ".format(cd=course_details_tbl)
    return exec_query(query)

def get_courses_some_cols():
    query = "select {course_number},{course_title},{description} from courses".\
        format(course_number=columns_names.course_number,
               course_title=columns_names.course_title, description=columns_names.description)
    return exec_query(query)

def insert_new_course_to_system(new_course_number,institution,title,topics_str,desc, web_link):
    query="INSERT INTO {courses_tbl} ({institut_col},{coursenum_col},{title_col},{topics_col},{desc_col},{weblink_col}) " \
          "VALUES (%s,'{coursenum_val}',%s,'{topics_val}',%s,%s);".\
        format(courses_tbl=courses_tbl, institut_col=columns_names.institution, coursenum_col=columns_names.course_number,
               title_col=columns_names.course_title, topics_col=columns_names.topics, desc_col=columns_names.description,
               weblink_col=web_link_col, coursenum_val=new_course_number, topics_val=topics_str)

    param_list = [institution, title, desc, web_link]
    return exec_non_query(query, None, tuple(param_list))

def get_countries():
    query = "select id,name from countries where name!='none'"
    return exec_query(query)


def get_users_demogr_details_df(spark=None,and_userid=None,update_status=1):
    extend_where=""
    if and_userid is not None:
        extend_where="and ({update_status_col}=1 or {userid_col}=\"{userid}\")".\
            format(update_status_col=general_update_status_col,userid_col=columns_names.user_id,userid=and_userid)
    elif update_status is not None:
        extend_where="and ({update_status_col}={update_status} and {clustre_col} is not null)".\
            format(update_status_col=general_update_status_col,update_status=update_status,clustre_col=columns_names.cluster)

    query_str="SELECT {user_id},{education},{gender},{YOB},{long},{lat} " \
              "FROM {view} where 1=1 {extend_where}".format(user_id=columns_names.user_id, education=columns_names.education, gender=columns_names.gender, YOB=columns_names.age,
               country=columns_names.country, long=columns_names.longitude,lat=columns_names.latitude, view=student_details_view,
               extend_where=extend_where)
    if spark is not None:
        spark_query="({query}) as temp".format(query=query_str)
        return fetch_df_from_large_sql_data(spark, spark_query, columns_names.age, 8)
    else:
        return fetch_df_by_query(query_str)

def get_demogr_details_by_user(user_id, toDF=False):
    query_str="SELECT {gender},{education},{YOB},{long},{lat} " \
              "FROM {view} where {user_id}='{user_id_val}' ".format(user_id=columns_names.user_id, education=columns_names.education,
                                              gender=columns_names.gender, YOB=columns_names.age,long=columns_names.longitude,
                                              lat=columns_names.latitude, view=student_details_view,user_id_val=user_id)
    if toDF == True:
        return fetch_df_by_query(query_str)
    else:
        return exec_query(query_str)

def get_all_course_id_student_id(spark):
    query_str="(SELECT {user_id},{course_id}, " \
              "ROW_NUMBER() OVER (ORDER BY {user_id}) row_num " \
              "FROM student_course) as temp".format(user_id=columns_names.user_id,
                                                    course_id=columns_names.course_id)
    return fetch_df_from_large_sql_data(spark, query_str, 'row_num', 8,0,1000000)

def get_all_course_id_student_id_panda():
    query_str="SELECT {user_id},{course_id} " \
              "FROM students_courses_copy".format(user_id=columns_names.user_id,
                                                    course_id=columns_names.course_id)
    return fetch_df_by_query(query_str)


def update_cluster_column_by_df(predictions_panda_df):
    query_str = 'ALTER TABLE {temp_table} ' \
                'CHANGE COLUMN `{user_id}` `{user_id}` VARCHAR(45) NOT NULL ; ' \
                'alter table MyOtherTable nocheck constraint all;' \
                'delete from {target_tbl}; ' \
                'alter table {target_tbl} check constraint all;' \
                'INSERT INTO {target_tbl} SELECT * FROM {temp_table};'\
        .format(temp_table=temp_table,user_id=columns_names.user_id, target_tbl="predictions")
    exec_query_panda(query_str, predictions_panda_df)


def update_clusters_by_df(predictions_df, spark=None):
    occurrence_num = "rn"
    sparkdf_query_str="select {userid},{prediction} as {cluster},1 from " \
              "(select userid_DI,prediction,row_number() OVER (PARTITION BY userid_DI ORDER BY userid_DI)  AS `{rn}` " \
              "from {temp_table} " \
              "group by userid_DI,prediction) t where {rn}=1".format(userid=columns_names.user_id,
                                                                     prediction=columns_names.prediction,
                                                                     cluster=columns_names.cluster,
                                                                     rn=occurrence_num,
                                                                     temp_table=temp_table)
    panda_df = None
    if spark is not None:
        predictions_df.registerTempTable(temp_table)
        panda_df = spark.sql(sparkdf_query_str).toPandas()
    else:
        panda_df = ps.sqldf("select {userid},predictions as {cluster},1 from predictions_df".
                            format(userid=columns_names.user_id, cluster=columns_names.cluster))
    query_str = "CALL sp_insert_predictions_from_temp()"
    exec_query_panda(query_str, panda_df)

def update_user_cluster(user_id,cluster_val):
    query = "INSERT INTO {pred_tbl} " \
            "({userid_col},{cluster_col}) " \
            "VALUES (\"{userid}\", {cluster}) ON DUPLICATE KEY UPDATE {cluster_col} = {cluster},{update_status}=0".\
        format(pred_tbl=predictions_table, userid_col=columns_names.user_id,cluster_col=columns_names.cluster,
               userid=user_id,cluster=cluster_val,update_status=general_update_status_col)
    return exec_non_query(query)


def get_users_same_cluster(userid, spark=None):
    query_str = "select  {predictions}.{useridcol} ,{predictions}.{cluster},{courseid} from " \
                "(select {cluster} from {predictions} where {useridcol}= \"{userid}\") t " \
                "inner join  {predictions} on t.{cluster}={predictions}.{cluster} " \
                "inner join {student_crs} on {predictions}.{useridcol}={student_crs}.{useridcol} " \
                "where ({predictions}.{cluster} is not null and {predictions}.{update_status_col}=1) " \
                "    or {predictions}.{useridcol}=\"{userid}\"".\
        format(predictions=predictions_table, useridcol=columns_names.user_id,
                cluster=columns_names.cluster, userid=userid,
                student_crs=student_course_table, courseid=columns_names.course_id,update_status_col=general_update_status_col)
    if spark is None:
        return fetch_df_by_query(query_str)
    else:
        query="({query}) as temp".format(query=query_str)
        return fetch_df_from_large_sql_data(spark, query, 1, 0, 1000)


def fetch_df_by_query(query):
    engine = create_engine(pymysql_engine_url)
    df = pd.read_sql(query, con=engine)
    return df


def fetch_df_from_large_sql_data(spark,push_down_query,partition_column,num_partitions=1,lower_bound=0,upper_bound=1):
    dataframe_mysql = spark.read.format("jdbc").option("url", "jdbc:mysql://"+DB_URL).option("driver", driver).\
    option("dbtable", push_down_query).option("user", DB_USER).option("password", DB_PASSWORD).\
    option("numPartitions", num_partitions).option("lowerBound", lower_bound).option("upperBound", upper_bound).option("partitionColumn", partition_column).load()
    #set numPartitions to 70 when student_course?
    return dataframe_mysql


def insert_from_df(data_df,table_name):
    engine = create_engine(pymysql_engine_url)
    df = pd.DataFrame(data_df)
    df.to_sql(table_name,con=engine,if_exists='append', index=False)


def exec_query_panda(query, data_panda_DF=None):
    engine = create_engine(pymysql_engine_url)
    try:
        if data_panda_DF is not None:
            data_panda_DF.to_sql(temp_table, engine, if_exists='replace', index=False)
    except TypeError as TE_error:
        print("error in data_panda_DF.to_sql(temp_table, engine, if_exists='replace', index=False)")
        print(TE_error)
    except ValueError as VE_error:
        print("error in data_panda_DF.to_sql(temp_table, engine, if_exists='replace', index=False)")
        print(VE_error)
    try:
        if query is not None:
            with engine.begin() as conn:
                conn.execute(query)
    except TypeError as TE_error:
        print("error in conn.execute the query: ", query)
        print(TE_error)
    except ValueError as VE_error:
        print("error in conn.execute the query: ", query)
        print(VE_error)


def insert_pyspark_df(df,table_name,query_str_before=None,query_str_after=None):
    exec_query_panda(query_str_before)
    df.write.format('jdbc').options(
      url= "jdbc:mysql://"+DB_URL,
      driver=driver,
      dbtable=table_name,
      user=DB_USER,
      password=DB_PASSWORD).mode('overwrite').save()
    if query_str_after is not None:
        exec_query_panda(query_str_before)


def create_mysql_connector():
    return mysql.connector.connect(host=DB_HOST,user=DB_USER,passwd=DB_PASSWORD,
                                   database=DB_NAME,auth_plugin='mysql_native_password')


def exec_query(query, params=None):
    conn = create_mysql_connector()
    cursor = conn.cursor(buffered=True, dictionary=True) # MySQLdb.cursors.DictCursor
    try:
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params)
        result = cursor.fetchall()
    except MySQLdb.Error as e:
        print("Error %d: %s", e.args[0], e.args[1])
    finally:
        cursor.close()
        conn.close()
    return result


def exec_non_query(query1, query2=None, params1=None, params2=None):
    conn = create_mysql_connector()
    num_rows_affected =0
    try:
        cursor = conn.cursor()
        if params1 is None:
            cursor.execute(query1)
        else:
            cursor.execute(query1, params1)
        num_rows_affected += cursor.rowcount
        if query2 is not None:
            cursor.execute(query2)
            num_rows_affected += cursor.rowcount
        conn.commit()
        '''
    except mysql.connector.Error as err:
        print("Something went wrong in mysql: {}".format(err))
    '''
    except MySQLdb.Error as e:
        conn.rollback()              # rollback transaction here
        print("Error %d: %s", e.args[0], e.args[1])
    finally:
        cursor.close()
        conn.close()
    return num_rows_affected > 0


def call_sp(sp_name, args_list=None):
    try:
        conn = create_mysql_connector()
        cursor = conn.cursor()
        if args_list is None:
            cursor.callproc(sp_name)
        else:
            cursor.callproc(sp_name, args_list)

        # print out the result
        for result in cursor.stored_results():
            print("query output: ")
            print(result.fetchall())
        conn.commit()
    except MySQLdb.Error as e:
        conn.rollback()              # rollback transaction here
        print("Error %d: %s", e.args[0], e.args[1])
    finally:
        cursor.close()
        conn.close()



if __name__ == '__main__':
    #insert_new_course_to_system("new_course","blarg'",'calrg',"math","'dasd\"das'")
    #print(get_courses("Math"))
    #print(get_users_demogr_details_df(None,"dankozak94@gmail.com"))
    print(get_demogr_details_by_user("MHxPC130251105"))
    #call_sp('sp_updateYoB',[1944, 'dan_ko@outlook108.com'])
    #call_sp('sp_updateLongLat',['dan_ko@outlook108.com'])



