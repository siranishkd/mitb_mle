import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def process_feature_gold(spark: SparkSession, datamart_silver_dir: str, datamart_gold_dir: str):
    """
    Construct the Gold Feature Store.
    Performs Point-in-Time aggregations to avoid Data Leakage.
    """
    print("--- Processing Feature Gold Tables ---")
    
    silver_click_path = os.path.join(datamart_silver_dir, "silver_feature_clickstream.parquet")
    silver_attr_path = os.path.join(datamart_silver_dir, "silver_feature_attributes.parquet")
    silver_fin_path = os.path.join(datamart_silver_dir, "silver_feature_financials.parquet")
    
    if not (os.path.exists(silver_click_path) and os.path.exists(silver_attr_path) and os.path.exists(silver_fin_path)):
        print("Silver tables not ready. Skipping Gold.")
        return
        
    df_click = spark.read.parquet(silver_click_path).drop("ingestion_timestamp")
    df_attr = spark.read.parquet(silver_attr_path).drop("ingestion_timestamp")
    df_fin = spark.read.parquet(silver_fin_path).drop("ingestion_timestamp")
    
    # We want to create features at the Customer_ID and snapshot_date level.
    # To prevent data leakage, we will define a rolling window for clickstream data
    # (e.g., aggregate clicks strictly prior to or on the snapshot_date).
    
    # Let's get the base anchor dates from the financials table (as it represents monthly snapshots)
    df_base = df_fin.select("Customer_ID", "snapshot_date").distinct()
    
    # 1. Join Attributes
    # We use the latest attribute record <= the base snapshot_date.
    # To keep it simple and robust against data leakage, we'll join on exact snapshot_date if they align.
    # Assuming attributes and financials are synced monthly. If not, a window function is needed.
    # Let's use a Window to get the most recent attribute for each base date.
    
    # Cross join strategy is too expensive. We can use a left join and fill forward using a Window, 
    # but exact join is safer if they share the same snapshot_date schedule.
    # We will assume they share the same monthly snapshot dates.
    df_feat = df_base.join(df_attr, ["Customer_ID", "snapshot_date"], "left")
    
    # Join Financials (which forms our base)
    df_feat = df_feat.join(df_fin, ["Customer_ID", "snapshot_date"], "left")
    
    # Feature Engineering on Financials
    if "Outstanding_Debt" in df_feat.columns and "Annual_Income" in df_feat.columns:
        df_feat = df_feat.withColumn("Debt_to_Income", col("Outstanding_Debt") / (col("Annual_Income") + 1))
        
    # 2. Clickstream Aggregations (Time-windowed)
    # We want sum of fe_1 over the last 90 days. 
    # Because clickstream could be daily, we group by Customer_ID, cast dates to timestamp, 
    # and use a rolling window of 90 days (90 * 86400 seconds).
    
    # Convert dates to unix timestamps for windowing
    df_click_ts = df_click.withColumn("ts", F.unix_timestamp("snapshot_date"))
    
    days_90 = 90 * 86400
    w = Window.partitionBy("Customer_ID").orderBy("ts").rangeBetween(-days_90, 0)
    
    df_click_agg = df_click_ts.select("Customer_ID", "snapshot_date", "ts", "fe_1", "fe_2") \
        .withColumn("fe_1_sum_90d", _sum("fe_1").over(w)) \
        .withColumn("fe_2_avg_90d", _avg("fe_2").over(w)) \
        .drop("ts")
    
    # Join the aggregated clickstream to our base feature table
    # We only take the clickstream aggregation exactly matching the snapshot_date of the base table
    df_gold = df_feat.join(df_click_agg.select("Customer_ID", "snapshot_date", "fe_1_sum_90d", "fe_2_avg_90d"), 
                           ["Customer_ID", "snapshot_date"], "left")
    
    # Fill NAs for missing clickstream
    df_gold = df_gold.fillna({"fe_1_sum_90d": 0, "fe_2_avg_90d": 0})
    
    out_path = os.path.join(datamart_gold_dir, "gold_feature_store.parquet")
    df_gold.write.mode("overwrite").parquet(out_path)
    print(f"Saved gold feature store to {out_path} with {df_gold.count()} rows.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldTest").getOrCreate()
    process_feature_gold(spark, "datamart/silver", "datamart/gold")
