from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import os
import json


def filteringForNetworkBusiness():
    """The idea is to get only the elite clients of 2017 and to create a json file where each row contains a user id
    and a business_id which are linked because the user wrote a review about that business"""
    spark = SparkSession.builder.getOrCreate();

    # ------------------------------------- FILTERING ELITE USER OF 2017 ------------------------------------------
    file_elite_users = os.listdir("../yelp_dataset/elite_users_with_friends.json")

    pyspark_df_elite_users = spark.read.json("../yelp_dataset/elite_users_with_friends.json/" + file_elite_users[0])

    for file in file_elite_users[1:]:
        df = spark.read.json("../yelp_dataset/elite_users_with_friends.json/" + file)
        pyspark_df_elite_users = pyspark_df_elite_users.union(df)

    # we now need to filter the elite users in order to get only those of 2017
    pyspark_df_elite_users = pyspark_df_elite_users.where("elite like '%2017%'")
    pyspark_df_elite_users.createOrReplaceTempView("elite_users")

    # --------------------------------------- CREATING THE REVIEWS DF ------------------------------------------
    file_open_business_reviews = os.listdir("../yelp_dataset/open_business_reviews.json")

    pyspark_df_reviews = spark.read.json("../yelp_dataset/open_business_reviews.json/" + file_open_business_reviews[0])

    for file in file_open_business_reviews[1:]:
        df = spark.read.json("../yelp_dataset/open_business_reviews.json/" + file)
        pyspark_df_reviews = pyspark_df_reviews.union(df)

    pyspark_df_reviews.createOrReplaceTempView("reviews")

    df = pyspark_df_elite_users.join(pyspark_df_reviews, pyspark_df_elite_users.user_id == pyspark_df_reviews.user_id).select(pyspark_df_elite_users['user_id'], pyspark_df_reviews['business_id'])

    df.write.mode('append').json("elite_and_business.json")




filteringForNetworkBusiness()
