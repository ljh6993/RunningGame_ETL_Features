"""
AWS Glue ETL Pipeline for TrumenApp Game Analytics
Processes raw game events from S3 and transforms them into analytics-ready format
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_S3_PATH',
    'OUTPUT_S3_PATH',
    'DRUID_S3_PATH',
    'PROCESSING_DATE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
INPUT_S3_PATH = args['INPUT_S3_PATH']  # s3://trumen-analytics-raw/events/
OUTPUT_S3_PATH = args['OUTPUT_S3_PATH']  # s3://trumen-analytics-processed/
DRUID_S3_PATH = args['DRUID_S3_PATH']   # s3://trumen-druid-deep-storage/
PROCESSING_DATE = args['PROCESSING_DATE']  # 2024-01-01

# Define schema for game events
event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device_info", StructType([
        StructField("device_id", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("network_type", StringType(), True),
    ]), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("accuracy", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("altitude", DoubleType(), True),
        StructField("heading", DoubleType(), True),
    ]), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

def process_location_exploration_events(df):
    """
    Process location exploration events for user behavior analysis
    """
    exploration_df = df.filter(col("event_type") == "location_exploration")
    
    # Extract tile information from metadata
    exploration_processed = exploration_df.select(
        col("timestamp"),
        col("user_id"),
        col("session_id"),
        col("device_info.platform").alias("platform"),
        col("device_info.app_version").alias("app_version"),
        col("location.latitude"),
        col("location.longitude"),
        col("location.accuracy"),
        col("location.speed"),
        col("metadata.tile_x").cast("int").alias("tile_x"),
        col("metadata.tile_y").cast("int").alias("tile_y"),
        col("metadata.tile_z").cast("int").alias("tile_z"),
        col("metadata.event_type").alias("exploration_type"),
        col("metadata.movement_speed").cast("double").alias("movement_speed"),
        # Convert timestamp to datetime
        from_unixtime(col("timestamp") / 1000).alias("event_datetime"),
        # Extract date parts for partitioning
        year(from_unixtime(col("timestamp") / 1000)).alias("year"),
        month(from_unixtime(col("timestamp") / 1000)).alias("month"),
        dayofmonth(from_unixtime(col("timestamp") / 1000)).alias("day"),
        hour(from_unixtime(col("timestamp") / 1000)).alias("hour")
    )
    
    return exploration_processed

def process_user_action_events(df):
    """
    Process user action events for feature usage analysis
    """
    action_df = df.filter(col("event_type") == "user_action")
    
    action_processed = action_df.select(
        col("timestamp"),
        col("user_id"),
        col("session_id"),
        col("device_info.platform").alias("platform"),
        col("device_info.app_version").alias("app_version"),
        col("metadata.action_type").alias("action_type"),
        # Convert timestamp to datetime
        from_unixtime(col("timestamp") / 1000).alias("event_datetime"),
        # Extract date parts
        year(from_unixtime(col("timestamp") / 1000)).alias("year"),
        month(from_unixtime(col("timestamp") / 1000)).alias("month"),
        dayofmonth(from_unixtime(col("timestamp") / 1000)).alias("day"),
        hour(from_unixtime(col("timestamp") / 1000)).alias("hour"),
        # Add additional metadata as JSON string
        to_json(col("metadata")).alias("metadata_json")
    )
    
    return action_processed

def process_suspicious_activity_events(df):
    """
    Process suspicious activity events for fraud detection
    """
    suspicious_df = df.filter(col("event_type") == "suspicious_activity")
    
    suspicious_processed = suspicious_df.select(
        col("timestamp"),
        col("user_id"),
        col("session_id"),
        col("device_info.platform").alias("platform"),
        col("location.latitude"),
        col("location.longitude"),
        col("metadata.suspicious_type").alias("suspicious_type"),
        col("metadata.risk_score").cast("double").alias("risk_score"),
        # Convert timestamp to datetime
        from_unixtime(col("timestamp") / 1000).alias("event_datetime"),
        # Extract date parts
        year(from_unixtime(col("timestamp") / 1000)).alias("year"),
        month(from_unixtime(col("timestamp") / 1000)).alias("month"),
        dayofmonth(from_unixtime(col("timestamp") / 1000)).alias("day"),
        hour(from_unixtime(col("timestamp") / 1000)).alias("hour"),
        # Add fraud details
        to_json(col("metadata")).alias("fraud_details")
    )
    
    return suspicious_processed

def create_user_session_metrics(location_df, action_df):
    """
    Create session-level metrics for user engagement analysis
    """
    # Session duration and tile exploration metrics
    session_location_metrics = location_df.groupBy("user_id", "session_id", "platform", "year", "month", "day").agg(
        min("timestamp").alias("session_start"),
        max("timestamp").alias("session_end"),
        count("*").alias("tiles_explored"),
        countDistinct(concat(col("tile_x"), lit("_"), col("tile_y"), lit("_"), col("tile_z"))).alias("unique_tiles"),
        avg("movement_speed").alias("avg_movement_speed"),
        max("movement_speed").alias("max_movement_speed"),
        # Calculate bounding box of exploration
        min("latitude").alias("min_lat"),
        max("latitude").alias("max_lat"),
        min("longitude").alias("min_lng"),
        max("longitude").alias("max_lng")
    ).withColumn(
        "session_duration_minutes", 
        (col("session_end") - col("session_start")) / (1000 * 60)
    ).withColumn(
        "exploration_efficiency",
        col("unique_tiles") / greatest(col("tiles_explored"), lit(1))
    )
    
    # User action metrics per session
    session_action_metrics = action_df.groupBy("user_id", "session_id", "platform", "year", "month", "day").agg(
        count("*").alias("total_actions"),
        countDistinct("action_type").alias("unique_action_types"),
        collect_list("action_type").alias("action_sequence")
    )
    
    # Combine session metrics
    session_metrics = session_location_metrics.join(
        session_action_metrics, 
        ["user_id", "session_id", "platform", "year", "month", "day"], 
        "left_outer"
    ).fillna(0, ["total_actions", "unique_action_types"])
    
    return session_metrics

def create_daily_user_aggregates(session_metrics_df):
    """
    Create daily user aggregates for dashboard consumption
    """
    daily_metrics = session_metrics_df.groupBy("user_id", "platform", "year", "month", "day").agg(
        count("session_id").alias("daily_sessions"),
        sum("tiles_explored").alias("daily_tiles_explored"),
        sum("unique_tiles").alias("daily_unique_tiles"),
        sum("session_duration_minutes").alias("daily_play_time_minutes"),
        avg("exploration_efficiency").alias("avg_exploration_efficiency"),
        max("max_movement_speed").alias("max_daily_speed"),
        sum("total_actions").alias("daily_total_actions"),
        # First and last activity of the day
        min("session_start").alias("first_activity"),
        max("session_end").alias("last_activity")
    ).withColumn(
        "tiles_per_minute",
        col("daily_tiles_explored") / greatest(col("daily_play_time_minutes"), lit(1))
    ).withColumn(
        "date",
        concat(col("year"), lit("-"), lpad(col("month"), 2, "0"), lit("-"), lpad(col("day"), 2, "0"))
    )
    
    return daily_metrics

def main():
    """
    Main ETL processing function
    """
    print(f"Starting ETL processing for date: {PROCESSING_DATE}")
    
    # Read raw events from S3
    input_path = f"{INPUT_S3_PATH}/year={PROCESSING_DATE[:4]}/month={PROCESSING_DATE[5:7]}/day={PROCESSING_DATE[8:10]}/"
    
    try:
        # Read JSON events
        raw_events_df = spark.read.option("multiLine", "true").json(input_path)
        
        print(f"Loaded {raw_events_df.count()} raw events")
        
        # Process different event types
        location_events = process_location_exploration_events(raw_events_df)
        action_events = process_user_action_events(raw_events_df)
        suspicious_events = process_suspicious_activity_events(raw_events_df)
        
        # Create aggregated metrics
        session_metrics = create_user_session_metrics(location_events, action_events)
        daily_metrics = create_daily_user_aggregates(session_metrics)
        
        # Write processed data to S3 in Parquet format for Druid ingestion
        
        # 1. Location events (partitioned by date and hour)
        location_output_path = f"{OUTPUT_S3_PATH}/location_events/"
        location_events.write.mode("overwrite").partitionBy("year", "month", "day", "hour").parquet(location_output_path)
        
        # 2. User action events
        action_output_path = f"{OUTPUT_S3_PATH}/user_actions/"
        action_events.write.mode("overwrite").partitionBy("year", "month", "day", "hour").parquet(action_output_path)
        
        # 3. Suspicious activity events
        suspicious_output_path = f"{OUTPUT_S3_PATH}/suspicious_activities/"
        suspicious_events.write.mode("overwrite").partitionBy("year", "month", "day", "hour").parquet(suspicious_output_path)
        
        # 4. Session metrics
        session_output_path = f"{OUTPUT_S3_PATH}/session_metrics/"
        session_metrics.write.mode("overwrite").partitionBy("year", "month", "day").parquet(session_output_path)
        
        # 5. Daily user aggregates
        daily_output_path = f"{OUTPUT_S3_PATH}/daily_user_metrics/"
        daily_metrics.write.mode("overwrite").partitionBy("year", "month", "day").parquet(daily_output_path)
        
        # Create Druid-optimized datasets
        create_druid_datasets(location_events, action_events, suspicious_events, daily_metrics)
        
        print("ETL processing completed successfully")
        
    except Exception as e:
        print(f"ETL processing failed: {str(e)}")
        raise e

def create_druid_datasets(location_df, action_df, suspicious_df, daily_df):
    """
    Create optimized datasets for Druid ingestion
    """
    
    # Real-time location events for live dashboards
    druid_location = location_df.select(
        col("timestamp").alias("__time"),
        col("user_id"),
        col("platform"),
        col("latitude"),
        col("longitude"),
        col("tile_x"),
        col("tile_y"),
        col("tile_z"),
        col("movement_speed"),
        col("accuracy")
    )
    
    druid_location_path = f"{DRUID_S3_PATH}/location_events/{PROCESSING_DATE}/"
    druid_location.coalesce(10).write.mode("overwrite").json(druid_location_path)
    
    # User actions for feature usage analysis
    druid_actions = action_df.select(
        col("timestamp").alias("__time"),
        col("user_id"),
        col("platform"),
        col("action_type"),
        col("app_version")
    )
    
    druid_actions_path = f"{DRUID_S3_PATH}/user_actions/{PROCESSING_DATE}/"
    druid_actions.coalesce(5).write.mode("overwrite").json(druid_actions_path)
    
    # Suspicious activities for fraud monitoring
    druid_suspicious = suspicious_df.select(
        col("timestamp").alias("__time"),
        col("user_id"),
        col("platform"),
        col("suspicious_type"),
        col("risk_score"),
        col("latitude"),
        col("longitude")
    )
    
    druid_suspicious_path = f"{DRUID_S3_PATH}/suspicious_activities/{PROCESSING_DATE}/"
    druid_suspicious.coalesce(2).write.mode("overwrite").json(druid_suspicious_path)
    
    # Daily aggregates for historical analysis
    druid_daily = daily_df.select(
        unix_timestamp(col("date")).alias("__time") * 1000,
        col("user_id"),
        col("platform"),
        col("daily_sessions"),
        col("daily_tiles_explored"),
        col("daily_unique_tiles"),
        col("daily_play_time_minutes"),
        col("tiles_per_minute"),
        col("max_daily_speed")
    )
    
    druid_daily_path = f"{DRUID_S3_PATH}/daily_metrics/{PROCESSING_DATE}/"
    druid_daily.coalesce(2).write.mode("overwrite").json(druid_daily_path)
    
    print("Druid datasets created successfully")

if __name__ == "__main__":
    main()

job.commit()