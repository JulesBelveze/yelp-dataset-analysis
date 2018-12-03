from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

dir_path = '../yelp_dataset/'

def getEliteUsers():

    #TO OPEN FILE AS DICTIONARY
    elite_users = {}
    with open("../elite_users.json") as f:
        for line in f:
           key = line.split()
           user = key[0]
           # print(user)
           elite_users[user] = 0

    return elite_users

def getFriends(elite_users):
    spark = SparkSession.builder.getOrCreate();

    spark_df_users = spark.read.json(dir_path + "yelp_academic_dataset_user.json")
    el_u_total = len(elite_users.keys())

    friends = []
    i = 0
    elite_friends = {}

    for user in elite_users:
        df_friends = spark_df_users.where(spark_df_users.user_id == user).select(spark_df_users.friends)
        friends = df_friends.take(df_friends.count())[0][0]
        friends_dict = {}
        for f in friends.split(','):
            friends_dict[f] = 0
        if friends_dict:
            with open("../elite_friends.json", "a") as file:
                file.write('%s:%s\n' % (user, friends_dict))
        i += 1
        print(i/el_u_total * 100, '%', ' – ', i, 'of', el_u_total, end='\r')
    print('Process of finding elite friends completed.')

# This program just collect all the Elite Users into one file

getFriends(getEliteUsers())
# TO OPEN FILE AS DICTIONARY
# d = {}
# with open("file.txt") as f:
#     for line in f:
#        (key, val) = line.split()
#        d[int(key)] = val
