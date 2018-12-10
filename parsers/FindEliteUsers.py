from pyspark.sql import SparkSession


def getEliteUsers():
    """function creating .json files each containing the informations of elite users of a specified year"""
    dir_path = '../yelp_dataset/'

    spark = SparkSession.builder.getOrCreate();

    spark_df_users = spark.read.json(dir_path + "yelp_academic_dataset_user.json")

    # filtering users without friends and who are not elite
    spark_df_users = spark_df_users.where("friends != 'None' AND elite != 'None'")

    # dropping useless columns
    spark_df_users = spark_df_users.drop("average_stars", "compliment_cool", "compliment_cute", "compliment_funny",
                                         "compliment_hot", "compliment_list", "compliment_more", "compliment_note",
                                         "compliment_photos", "compliment_plain", "compliment_profile",
                                         "compliment_writer", "cool", "funny", "useful", "yelping_since")

    # filtering users by some criterion to get the most influential ones
    spark_df_users = spark_df_users.where(spark_df_users.fans > 10)
    spark_df_users = spark_df_users.where(spark_df_users.review_count > 50)

    # sorting them by number of fans
    spark_df_users.sort('fans', ascending=True).show()

    # writing the dataframes as .json files
    spark_df_users_14 = spark_df_users.where(spark_df_users.elite.like('2014'))
    spark_df_users_14.write.format('json').save("elite_users_2014.json")

    spark_df_users_15 = spark_df_users.where(spark_df_users.elite.like('2015'))
    spark_df_users_15.write.format('json').save("elite_users_2015.json")

    spark_df_users_16 = spark_df_users.where(spark_df_users.elite.like('2016'))
    spark_df_users_16.write.format('json').save("elite_users_2016.json")

    spark_df_users_17 = spark_df_users.where(spark_df_users.elite.like('2017'))
    spark_df_users_17.write.format('json').save("elite_users_2017.json")


if __name__ == "__main__":
    getEliteUsers()
