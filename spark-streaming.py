# import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
from env import POSTGRES_JDBC_LOCATION, CHECKPOINT_LOCATION_1, CHECKPOINT_LOCATION_2

if __name__ == "__main__":
    # print(pyspark.__version__)

    # initialize spark session
    spark = (SparkSession.builder
             .appName("RealTimeVotingEngineering")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
             .config("spark.jars", POSTGRES_JDBC_LOCATION)  # postgres driver
             # disable adaptive query execution
             .config("spark.sql.adaptive.enable", "false")
             .getOrCreate()
             )

    # spark.sparkContext.setLogLevel("DEBUG")

    # define schema for vote data
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True),
        ]), True),

        StructField("voter_id", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True),
    ])

    # read vote data from kafka
    votes_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "voters_topic")
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), vote_schema).alias("data"))
                .select("data.*")
                )

    # data pre-processing: typecasting and watermarking
    votes_df = votes_df.withColumn(
        "voting_time", col('voting_time').cast(TimestampType())) \
        .withColumn("vote", col("vote").cast(IntegerType()))

    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")

    # aggregate votes per candidate and turnout by location
    votes_per_candidate = enriched_votes_df.groupBy(
        "candidate_id", "candidate_name", "party_affiliation", "photo_url").agg(_sum('vote').alias("total_votes"))

    turnout_by_location = enriched_votes_df.groupBy(
        'address.state').count().alias("total_turnout_votes")

    # write aggregated data to kafka

    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr(
        "to_json(struct(*)) AS value")
        .writeStream.format('kafka')
        .option('kafka.bootstrap.servers', 'localhost:9092')
        .option('topic', 'aggregated_votes_per_candidate')
        # checkpoint is to prevent data loss and preprocessing of existing data
        .option('checkpointLocation', CHECKPOINT_LOCATION_1)
        .outputMode('update')
        .start()
    )

    turnout_by_location_to_kafka = (turnout_by_location.selectExpr(
        "to_json(struct(*)) AS value")
        .writeStream.format('kafka')
        .option('kafka.bootstrap.servers', 'localhost:9092')
        .option('topic', 'aggregated_turnout_by_location')
        # checkpoint is to prevent data loss and preprocessing of existing data
        .option('checkpointLocation', CHECKPOINT_LOCATION_2)
        .outputMode('update')
        .start()
    )

    # await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()
