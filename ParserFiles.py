from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate();  # creating a Sparksession

df_review = spark.read.json("../yelp_dataset/yelp_academic_dataset_review.json")
df_business = spark.read.json("../yelp_dataset/yelp_academic_dataset_business.json")

df_review.printSchema()

df_review.createOrReplaceTempView("review")
df_review.createOrReplaceTempView("review2")
df_business.createOrReplaceTempView("business")

sqlDF = spark.sql("SELECT review.business_id, review.date, review.review_id, review.stars, review.text, review.user_id FROM (SELECT business_id, COUNT (*) AS count_business FROM review2 GROUP BY business_id) AS A INNER JOIN review ON review.business_id = A.business_id WHERE count_business BETWEEN 100 AND 300 AND review.date > '2016-01-01'")

sqlDF.show()

sqlDF.write.format('json').save('reviews_2.json')
