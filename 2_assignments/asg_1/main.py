import os
import pyspark
from pyspark.sql import SparkSession

def main():
    print("Starting ETL Pipeline for Assignment 1...")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ASG1_Medallion_Pipeline") \
        .getOrCreate()
    
    # Create datamart directories if they don't exist
    directories = ["datamart/bronze", "datamart/silver", "datamart/gold"]
    for d in directories:
        os.makedirs(d, exist_ok=True)
        print(f"Ensured directory exists: {d}")
        
    # TODO: Bronze Pipeline
    print("Running Bronze Pipeline...")
    
    # TODO: Silver Pipeline
    print("Running Silver Pipeline...")
    
    # TODO: Gold Pipeline
    print("Running Gold Pipeline...")
    
    print("Pipeline completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
