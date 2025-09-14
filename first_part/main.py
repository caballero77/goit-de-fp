import findspark
findspark.init()
import json
import logging
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, count, when, isnan, isnull, 
    from_json, to_json, struct, lit, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, DateType, LongType
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import to_date
import os
import subprocess
from config import MYSQL_CONFIG, KAFKA_CONFIG
from confluent_kafka.admin import AdminClient, NewTopic

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Main execution starts here
try:
    logger.info("Starting Olympic Athlete Streaming Processor...")
    
    # Create Spark session
    logger.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("OlympicAthleteStreaming") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars", "/app/mysql-connector-j-8.0.32.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session created successfully")
    
    # Create Kafka topics if they don't exist
    logger.info("Creating Kafka topics if they don't exist...")
    
    def create_kafka_topic(topic_name):
        """Create Kafka topic if it doesn't exist"""
        try:
            # Create AdminClient
            admin_client = AdminClient(
                {
                    "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": KAFKA_CONFIG["username"],
                    "sasl.password": KAFKA_CONFIG["password"],
                }
            )
            
            # Check if topic exists
            metadata = admin_client.list_topics(timeout=10)
            if topic_name in metadata.topics:
                logger.info(f"Topic '{topic_name}' already exists")
                return True
            
            # Create topic if it doesn't exist
            new_topic = NewTopic(topic_name, num_partitions=2, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            
            # Wait for topic creation to complete
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"Successfully created topic '{topic_name}'")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to create topic '{topic_name}': {str(e)}")
                    return False
                    
        except Exception as e:
            logger.warning(f"Error creating topic '{topic_name}': {str(e)}")
            return False
    
    def delete_kafka_topic(topic_name):
        """Delete Kafka topic if it exists"""
        try:
            # Create AdminClient
            admin_client = AdminClient(
                {
                    "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": KAFKA_CONFIG["username"],
                    "sasl.password": KAFKA_CONFIG["password"],
                }
            )
            
            # Check if topic exists
            metadata = admin_client.list_topics(timeout=10)
            if topic_name not in metadata.topics:
                logger.info(f"Topic '{topic_name}' does not exist, skipping deletion")
                return True
            
            # Delete topic if it exists
            fs = admin_client.delete_topics([topic_name])
            
            # Wait for topic deletion to complete
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"Successfully deleted topic '{topic_name}'")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to delete topic '{topic_name}': {str(e)}")
                    return False
                    
        except Exception as e:
            logger.warning(f"Error deleting topic '{topic_name}': {str(e)}")
            return False
    
    # Create input and output topics
    input_topic_created = create_kafka_topic(KAFKA_CONFIG['input_topic'])
    output_topic_created = create_kafka_topic(KAFKA_CONFIG['output_topic'])
    
    if not input_topic_created or not output_topic_created:
        logger.warning("Some topics could not be created. Continuing anyway...")
    
    # Read athlete bio data from MySQL
    logger.info("Reading athlete bio data from MySQL...")
    
    # Define the schema for athlete bio data
    athlete_bio_schema = StructType([
        StructField("athlete_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("born", StringType(), True),
        StructField("height", StringType(), True),
        StructField("weight", StringType(), True),
        StructField("country", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("description", StringType(), True),
        StructField("special_notes", StringType(), True),
    ])
    
    # Read from MySQL
    athlete_bio_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}") \
        .option("dbtable", "olympic_dataset.athlete_bio") \
        .option("user", MYSQL_CONFIG['user']) \
        .option("password", MYSQL_CONFIG['password']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .schema(athlete_bio_schema) \
        .load()
    
    # Filter out empty or non-numeric height/weight data
    filtered_df = athlete_bio_df.filter(
        col("height").isNotNull() & 
        col("weight").isNotNull() & 
        ~isnan(col("height")) & 
        ~isnan(col("weight")) &
        (col("height") > 0) &
        (col("weight") > 0)
    )

    # Format born date from 4April1949 to 1949-04-04
    athlete_bio_df = filtered_df.withColumn("born", to_date(col("born"), "ddMMMyyyy")) \
        .withColumn("height", col("height").cast(DoubleType())) \
        .withColumn("weight", col("weight").cast(DoubleType()))
    
    logger.info(f"Loaded {athlete_bio_df.count()} athlete records after filtering")
    
    # Optional: Read and write athlete event results to Kafka (one-time operation)
    # Uncomment the following section if you need to populate Kafka topic
    
    logger.info("Reading athlete event results from MySQL and writing to Kafka...")
    
    # Define schema for event results (matching actual MySQL schema)
    event_results_schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", IntegerType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", LongType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True),
    ])
    
    # Read from MySQL
    event_results_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}") \
        .option("dbtable", "olympic_dataset.athlete_event_results") \
        .option("user", MYSQL_CONFIG['user']) \
        .option("password", MYSQL_CONFIG['password']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .schema(event_results_schema) \
        .load()
    
    # Convert to JSON and write to Kafka
    event_results_json = event_results_df.select(
        to_json(struct("*")).alias("value")
    )
    
    event_results_json.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
        .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config", KAFKA_CONFIG["sasl_jaas_config"]) \
        .option("topic", KAFKA_CONFIG['input_topic']) \
        .save()
    
    logger.info("Successfully wrote athlete event results to Kafka topic")
    
    # Start streaming data processing
    logger.info("Starting streaming data processing...")
    
    # Define schema for JSON data from Kafka (matching actual MySQL schema)
    json_schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", IntegerType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", LongType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True),
    ])
    
    # Read streaming data from Kafka
    streaming_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
        .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config", KAFKA_CONFIG["sasl_jaas_config"]) \
        .option("subscribe", KAFKA_CONFIG['input_topic']) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "5") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON data
    parsed_df = streaming_df.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select("data.*")
    
    # Join streaming data with athlete bio data
    logger.info("Joining streaming data with athlete bio data...")
    joined_df = parsed_df.join(
        athlete_bio_df.select("athlete_id", "name", "sex", "born", "height", "weight", "country", col("country_noc").alias("athlete_country_noc")), 
        on="athlete_id", 
        how="inner"
    )
    
    # Calculate averages
    logger.info("Calculating average height and weight statistics...")
    aggregated_df = joined_df.groupBy(
        "sport", "medal", "sex", "athlete_country_noc"
    ).agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        count("*").alias("athlete_count"),
        current_timestamp().alias("calculation_timestamp")
    )
    
    # Define batch processing function inline
    def process_batch(df, epoch_id):
        if df.count() > 0:
            logger.info(f"Processing batch {epoch_id} with {df.count()} records")
            
            # Write to Kafka
            logger.info(f"Writing batch {epoch_id} to Kafka output topic...")
            kafka_df = df.select(
                to_json(struct("*")).alias("value")
            )
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
                .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
                .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
                .option("kafka.sasl.jaas.config", KAFKA_CONFIG["sasl_jaas_config"]) \
                .option("topic", KAFKA_CONFIG['output_topic']) \
                .save()
            
            # Write to database
            logger.info(f"Writing batch {epoch_id} to MySQL database...")
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}") \
                .option("dbtable", "athlete_statistics_maslianko_andrii") \
                .option("user", MYSQL_CONFIG['user']) \
                .option("password", MYSQL_CONFIG['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("append") \
                .save()
        else:
            logger.info(f"Batch {epoch_id} is empty, skipping...")

    # Process streaming data with forEachBatch
    query = aggregated_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()
    
    logger.info("Streaming query started. Waiting for termination...")
    query.awaitTermination()

except Exception as e:
    logger.error(f"Error in processing: {str(e)}")
    raise
finally:
    # Clean up Kafka topics
    logger.info("Cleaning up Kafka topics...")
    try:
        delete_kafka_topic(KAFKA_CONFIG['input_topic'])
        delete_kafka_topic(KAFKA_CONFIG['output_topic'])
    except Exception as e:
        logger.warning(f"Error during topic cleanup: {str(e)}")
    
    if 'spark' in locals():
        spark.stop()
        logger.info("Spark session stopped")