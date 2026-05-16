import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def process_feature_bronze(spark: SparkSession, datamart_bronze_dir: str):
    """
    Ingest raw feature CSVs and save them as Bronze Parquet tables.
    """
    print("--- Processing Feature Bronze Tables ---")
    
    files_to_process = {
        "clickstream": "data/feature_clickstream.csv",
        "attributes": "data/features_attributes.csv",
        "financials": "data/features_financials.csv"
    }
    
    for name, path in files_to_process.items():
        if not os.path.exists(path):
            print(f"Warning: {path} not found.")
            continue
            
        print(f"Ingesting {name} from {path}...")
        
        # Read as strings to prevent schema inference issues on messy data
        df = spark.read.csv(path, header=True, inferSchema=False)
        
        # Add ingestion metadata
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        out_path = os.path.join(datamart_bronze_dir, f"bronze_feature_{name}.parquet")
        
        # Write to Parquet
        df.write.mode("overwrite").parquet(out_path)
        print(f"Saved {name} to {out_path} with {df.count()} rows.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeTest").getOrCreate()
    process_feature_bronze(spark, "datamart/bronze")
