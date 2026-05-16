import os
import pyspark
from pyspark.sql import SparkSession

from utils.feature_bronze_table import process_feature_bronze
from utils.feature_silver_table import process_feature_silver
from utils.feature_gold_table import process_feature_gold
# We can also call the Lab 2 scripts if needed, but for ASG 1 we focus on the feature store
# from utils.data_processing_bronze_table import process_bronze_table
# from utils.data_processing_silver_table import process_silver_table
# from utils.data_processing_gold_table import process_labels_gold_table

def main():
    print("Starting ETL Pipeline for Assignment 1...")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ASG1_Medallion_Pipeline") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Create datamart directories if they don't exist
    directories = ["datamart/bronze", "datamart/silver", "datamart/gold"]
    for d in directories:
        os.makedirs(d, exist_ok=True)
        print(f"Ensured directory exists: {d}")
        
    print("\n--- Running Bronze Pipeline ---")
    process_feature_bronze(spark, "datamart/bronze")
    
    print("\n--- Running Silver Pipeline ---")
    process_feature_silver(spark, "datamart/bronze", "datamart/silver")
    
    print("\n--- Running Gold Pipeline ---")
    process_feature_gold(spark, "datamart/silver", "datamart/gold")
    
    print("\nPipeline completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
