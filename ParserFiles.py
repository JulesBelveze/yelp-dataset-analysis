from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import os


def parserByReviewsNb():
    spark = SparkSession.builder.getOrCreate();  # creating a Sparksession
    sqlContext = SQLContext(spark)

    file_list = os.listdir("../yelp_dataset/open_business_reviews.json")
    pyspark_df_review = spark.read.json("../yelp_dataset/open_business_reviews.json/" + file_list[0])

    for file in file_list[1:]:
        df = spark.read.json("../yelp_dataset/open_business_reviews.json/" + file)
        pyspark_df_review = pyspark_df_review.union(df)

    # p_schema = StructType([StructField('business_id', StringType(), True),
    #                        StructField('cool', IntegerType(), True),
    #                        StructField('date', StringType(), True),
    #                        StructField('funny', IntegerType(), True),
    #                        StructField('review_id', StringType(), True),
    #                        StructField('stars', IntegerType(), True),
    #                        StructField('text', StringType(), True),
    #                        StructField('useful', IntegerType(), True),
    #                        StructField('user_id', StringType(), True)])
    #
    # pyspark_df_review = sqlContext.createDataFrame(df_review, p_schema)

    pyspark_df_review.createOrReplaceTempView("review")
    pyspark_df_review.createOrReplaceTempView("review2")

    sqlDF = spark.sql(
        "SELECT review.business_id, review.date, review.review_id, review.stars, review.text, review.user_id FROM (SELECT business_id, COUNT (*) AS count_business FROM review2 GROUP BY business_id) AS A INNER JOIN review ON review.business_id = A.business_id WHERE count_business BETWEEN 100 AND 300 AND review.date > '2017-01-01'")

    sqlDF.show()

    sqlDF.write.format('json').save('reviews_filter.json')
    pass


def removeClosedBusiness():
    spark = SparkSession.builder.getOrCreate();  # creating a Sparksession
    df_business = spark.read.json("../yelp_dataset/yelp_academic_dataset_business.json")

    df_business.printSchema()

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

    spark_df_review = spark.read.json("../yelp_dataset/yelp_academic_dataset_review.json")
    reviews_filter = spark_df_review.filter(spark_df_review['business_id'].isin(business_list) == True)

    reviews_filter.write.format('json').save('open_business_reviews.json')
    pass


def getEliteWithFriends():
    spark = SparkSession.builder.getOrCreate();

    spark_df_users = spark.read.json("../yelp_dataset/yelp_academic_dataset_user.json")
    spark_df_users.printSchema()

    spark_df_users = spark_df_users.where("friends != 'None' AND elite != 'None'")
    spark_df_users = spark_df_users.drop("average_stars", "compliment_cool", "compliment_cute", "compliment_funny",
                                         "compliment_hot", "compliment_list", "compliment_more", "compliment_note",
                                         "compliment_photos", "compliment_plain", "compliment_profile",
                                         "compliment_writer", "cool", "funny", "useful", "yelping_since")
    # spark_df_users.write.format("json").save("elite_users_with_friends.json")
    pass


def getEliteWithEliteFriends():
    def filterFriend(elt, l):
        print(elt)
        # return elt if len(set(elt) & set(l)) > 0 else None

    spark = SparkSession.builder.getOrCreate();
    sqlContext = SQLContext(spark);

    spark_df_users = spark.read.json("../yelp_dataset/yelp_academic_dataset_user.json")

    spark_df_users = spark_df_users.where("friends != 'None' AND elite != 'None'")
    spark_df_users = spark_df_users.drop("average_stars", "compliment_cool", "compliment_cute", "compliment_funny",
                                         "compliment_hot", "compliment_list", "compliment_more", "compliment_note",
                                         "compliment_photos", "compliment_plain", "compliment_profile",
                                         "compliment_writer", "cool", "funny", "useful", "yelping_since")

    spark_df_users = spark_df_users.withColumn("friends", split(col("friends"), "\\|"))

    list_users = []

    # for i in list_users.collect():
    #     print(str(i.friends))
    def removePunctuation(df, column):
        cleanString = df.select(trim(lower(col('user_id'))).alias('user_id'))
        cleanString = cleanString.select(filterFriend(user_id, ['Ha3iJu77CxlrFm-vQRs_8g']).alias('user_id'))

        return cleanString

    result = removePunctuation(spark_df_users, 'user_id')

    pass


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


getEliteWithEliteFriends()
