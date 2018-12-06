from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

dir_path = '../yelp_dataset/'

def getEliteUsers():
    spark = SparkSession.builder.getOrCreate();

    spark_df_users = spark.read.json(dir_path + "yelp_academic_dataset_user.json")

    spark_df_users = spark_df_users.where("friends != 'None' AND elite != 'None'")
    spark_df_users = spark_df_users.drop("average_stars", "compliment_cool", "compliment_cute", "compliment_funny",
                                         "compliment_hot", "compliment_list", "compliment_more", "compliment_note",
                                         "compliment_photos", "compliment_plain", "compliment_profile",
                                         "compliment_writer", "cool", "funny", "useful", "yelping_since")

    el_u_total = spark_df_users.count()
    print("Total elite users: ", el_u_total)
    spark_df_users = spark_df_users.where(spark_df_users.elite.like('2016'))
    spark_df_users = spark_df_users.where(spark_df_users.fans > 10)
    spark_df_users = spark_df_users.where(spark_df_users.review_count > 50)
    spark_df_users.sort('fans', ascending=True).show()
    el_u_filtered = spark_df_users.count()
    print("Only filtered elite users: ", el_u_filtered, el_u_filtered/el_u_total * 100, '% of the total users.')

    elite_users_array = spark_df_users.select('user_id').take(el_u_filtered)

    i = 0
    for el_u in elite_users_array:
        user = el_u[0]
        with open("../elite_users_2016.json", "a") as file:
            file.write('%s\n' % user)
        i += 1
        print(i/el_u_filtered * 100, '%', end='\r')
    print('Process of finding Elite users completed')

# This program just collect all the Elite Users into one file
getEliteUsers()

# TO OPEN FILE AS DICTIONARY
# d = {}
# with open("file.txt") as f:
#     for line in f:
#        (key, val) = line.split()
#        d[int(key)] = val
