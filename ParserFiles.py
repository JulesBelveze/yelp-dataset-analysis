from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import os
import json

test = True
dir_path = '../yelp_dataset/'


def parserByReviewsNb():
    spark = SparkSession.builder.getOrCreate();  # creating a Sparksession

    file_list = os.listdir(dir_path + "open_business_reviews.json")
    pyspark_df_review = spark.read.json(dir_path + "open_business_reviews.json/" + file_list[0])

    for file in file_list[1:]:
        df = spark.read.json(dir_path + "open_business_reviews.json/" + file)
        pyspark_df_review = pyspark_df_review.union(df)

    pyspark_df_review.createOrReplaceTempView("review")
    pyspark_df_review.createOrReplaceTempView("review2")

    sqlDF = spark.sql(
        "SELECT review.business_id, review.date, review.review_id, review.stars, review.text, review.user_id FROM (SELECT business_id, COUNT (*) AS count_business FROM review2 GROUP BY business_id) AS A INNER JOIN review ON review.business_id = A.business_id WHERE count_business BETWEEN 100 AND 300 AND review.date > '2017-01-01'")

    sqlDF.show()

    sqlDF.write.format('json').save('reviews_filter.json')
    pass


def removeClosedBusiness():
    spark = SparkSession.builder.getOrCreate();  # creating a Sparksession
    df_business = spark.read.json(dir_path + "yelp_academic_dataset_business.json")

    df_business.printSchema()
    df_business = df_business.drop("attributes")

    df_business.createOrReplaceTempView("business")
    sqlDF = spark.sql("SELECT * FROM business WHERE is_open = 1")
    sqlDF.show()

    sqlDF.write.format('json').save('open_business.json')
    pass


def removeClosedBusinessReviews():
    spark = SparkSession.builder.getOrCreate();

    df_business = pd.DataFrame()
    file_list = os.listdir("../open_business.json")

    for file in file_list:
        # loading the business into a dataframe
        df = pd.read_json("../open_business.json/" + file, lines=True)
        df_business = pd.concat([df_business, df])

    business_list = list(set(list(df_business.business_id.values)))

    spark_df_review = spark.read.json(dir_path + "yelp_academic_dataset_review.json")
    reviews_filter = spark_df_review.filter(spark_df_review['business_id'].isin(business_list) == True)

    reviews_filter.write.format('json').save('open_business_reviews.json')
    pass


def getOpenBusinessClients():
    df_reviews = pd.DataFrame()
    file_list = os.listdir("../yelp_dataset/open_business_reviews.json")

    for file in file_list:
        # loading the business into a dataframe
        df = pd.read_json("../yelp_dataset/open_business_reviews.json/" + file, lines=True)
        df_reviews = pd.concat([df_reviews, df])

    dic = {elt: [] for elt in list(df_reviews.user_id.values)}

    for i in range(len(df_reviews)):
        dic[df_reviews.loc[i].user_id].append(df_reviews.loc[i].business_id)

    with open('../yelp_dataset/business_users.json', 'w') as f:
        json.dump(dic, f, indent=4)


def getOpenBusinessEliteClients():
    df_users = pd.DataFrame()
    df_reviews = pd.DataFrame()

    file_reviews_list = os.listdir("../yelp_dataset/open_business_reviews.json")
    file_user_list = os.listdir("../yelp_dataset/elite_users_with_friends.json")

    for file in file_reviews_list:
        # loading the business into a dataframe
        df = pd.read_json("../yelp_dataset/open_business_reviews.json/" + file, lines=True)
        df_reviews = pd.concat([df_reviews, df])

    for file in file_user_list:
        # loading the elite users into a dataframe
        df = pd.read_json("../yelp_dataset/elite_users_with_friends.json/" + file, lines=True)
        df_users = pd.concat([df_users, df])

    dic = {}
    # dic = {elt: [] for elt in list(df_reviews.user_id.values) if df_users[df_users["user_id"] == elt].elite.values != []}

    for i in range(len(df_reviews)):
        user = df_reviews.loc[i].user_id
        if df_users[df_users['user_id'] == user].elite.values != 'None':
            try:
                dic[df_reviews.loc[i].user_id].append(df_reviews.loc[i].business_id)
            except KeyError:
                dic[df_reviews.loc[i].user_id] = [df_reviews.loc[i].business_id]

    with open('../yelp_dataset/business_elite_users.json', 'w') as f:
        json.dump(dic, f, indent=4)


def getUsersWithFriends():
    spark = SparkSession.builder.getOrCreate();

    spark_df_users = spark.read.json("../yelp_dataset/yelp_academic_dataset_user.json")
    spark_df_users = spark_df_users.where("friends != 'None'")
    spark_df_users = spark_df_users.drop("average_stars", "compliment_cool", "compliment_cute", "compliment_funny",
                                         "compliment_hot", "compliment_list", "compliment_more", "compliment_note",
                                         "compliment_photos", "compliment_plain", "compliment_profile",
                                         "compliment_writer", "cool", "funny", "useful", "yelping_since")

    spark_df_users.write.format("json").save("users_with_friends.json")
    pass


if __name__ == "__main__":
    parserByReviewsNb()
