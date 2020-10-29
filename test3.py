from flask import Flask, render_template,request, send_from_directory, jsonify
from json import JSONEncoder
from DataAccess import get_topics, get_courses_some_cols,get_courses_details,get_countries,update_user,\
    update_user_courses, get_courses, get_users_same_cluster,insert_new_course_to_system
import threading
from threading import Timer
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from BL.create_clusters import Cluster
from BL import columns_names
from BL.hybrid import Hybrid
from BL.data_matrix import DataMatrix
from datetime import datetime
import findspark
import os
import time
import DataAccess



print(DataAccess.get_user_courses("MHxPC130024894"))