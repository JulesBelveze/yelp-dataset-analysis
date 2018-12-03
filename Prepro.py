from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

dir_path = '../yelp_dataset/'

def findGroup():
    spark = SparkSession.builder.getOrCreate();

    spark_df_users = spark.read.json(dir_path + "yelp_academic_dataset_user.json")

    spark_df_users = spark_df_users.where("friends != 'None' AND elite != 'None'")
    spark_df_users = spark_df_users.drop("average_stars", "compliment_cool", "compliment_cute", "compliment_funny",
                                         "compliment_hot", "compliment_list", "compliment_more", "compliment_note",
                                         "compliment_photos", "compliment_plain", "compliment_profile",
                                         "compliment_writer", "cool", "funny", "useful", "yelping_since")
    # spark_df_users.write.format("json").save("elite_users_with_friends.json")

    #spark_df_users = spark_df_users.withColumn("friends", split(col("friends"), "\\|"))
    el_u_total = spark_df_users.count()
    spark_df_users = spark_df_users.where(spark_df_users.elite.like('2017'))
    spark_df_users = spark_df_users.where(spark_df_users.fans > 10)
    spark_df_users = spark_df_users.where(spark_df_users.review_count > 50)
    spark_df_users.sort('fans', ascending=True).show()
    print("Total elite users: ", el_u_total)
    el_u_filtered = spark_df_users.count()
    print("Only filtered elite users: ", el_u_filtered)

findGroup()
