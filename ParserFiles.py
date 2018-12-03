from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import os
import FindEliteUsers.py

test = True
dir_path = '../yelp_dataset/'


def parserByReviewsNb():
    spark = SparkSession.builder.getOrCreate();  # creating a Sparksession
    sqlContext = SQLContext(spark)

    file_list = os.listdir(dir_path + "open_business_reviews.json")
    pyspark_df_review = spark.read.json(dir_path + "open_business_reviews.json/" + file_list[0])

    for file in file_list[1:]:
        df = spark.read.json(dir_path + "open_business_reviews.json/" + file)
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
    df_business = spark.read.json(dir_path + "yelp_academic_dataset_business.json")

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

    spark_df_review = spark.read.json(dir_path + "yelp_academic_dataset_review.json")
    reviews_filter = spark_df_review.filter(spark_df_review['business_id'].isin(business_list) == True)

    reviews_filter.write.format('json').save('open_business_reviews.json')
    pass


def getEliteWithEliteFriends():
    def filterFriend(elt, l):
        print(elt)
        # return elt if len(set(elt) & set(l)) > 0 else None

    spark = SparkSession.builder.getOrCreate();
    sqlContext = SQLContext(spark);

    #The following line extracts the elite users with friends
    spark_df_users = getEliteWithFriends()

    #Replace the column 'friends' with an array containing the friends
    spark_df_users = spark_df_users.withColumn("friends", split(col("friends"), "\\|"))

    elite_users = spark_df_users.select('user_id').take(spark_df_users.count())
    elite_users_dict = {user[0] for user in elite_users}
    # print(elite_users_dict)
    elite_friends = {}
    i = 0

    for user in elite_users_dict:
        # print(user)
        df_friends = spark_df_users.where(spark_df_users.user_id == user).select(spark_df_users.friends)
        friend_dict = {}
        for friend_list in df_friends.take(df_friends.count())[0]:
            #print(friend_list)
            for friend in friend_list:
                #print(friend)
                friend_dict = {f.strip() for f in friend.split(',')}

        #print('All friends', friend_dict)
        elite_friends[user] = {friend for friend in friend_dict if friend in elite_users_dict}

        #print('Only elite friends', elite_friends[user])
        with open("../elite_friends.json", "a") as file:
            file.write('%s:%s\n' % (user, elite_friends[user]))
        i += 1
        print(i)

    print('End of process')
        # TO RECOVER DATA FROM THE PREVIUOS FILE
        # data = dict()
        # with open("../elite_friends.json") as raw_data:
            # for item if raw_data:
    #         if ':' in item:
    #             key,value = item.split(':', 1)
    #             data[key]=value
    #         else:
    #             pass # deal with bad lines of text here
        #
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
