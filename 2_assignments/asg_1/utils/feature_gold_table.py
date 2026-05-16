import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, last as _last
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def process_feature_gold(spark: SparkSession, datamart_silver_dir: str, datamart_gold_dir: str):
    """
    Construct the Gold Feature Store.
    Performs Point-in-Time aggregations and ASOF joins to avoid Data Leakage.
    """
    print("--- Processing Feature Gold Tables (v2.0 ASOF) ---")
    
    silver_click_path = os.path.join(datamart_silver_dir, "silver_feature_clickstream.parquet")
    silver_attr_path = os.path.join(datamart_silver_dir, "silver_feature_attributes.parquet")
    silver_fin_path = os.path.join(datamart_silver_dir, "silver_feature_financials.parquet")
    
    if not (os.path.exists(silver_click_path) and os.path.exists(silver_attr_path) and os.path.exists(silver_fin_path)):
        print("Silver tables not ready. Skipping Gold.")
        return
        
    df_click = spark.read.parquet(silver_click_path).drop("ingestion_timestamp")
    df_attr = spark.read.parquet(silver_attr_path).drop("ingestion_timestamp")
    df_fin = spark.read.parquet(silver_fin_path).drop("ingestion_timestamp")
    
    # Let's get the base anchor dates from the financials table
    # This acts as the universe of Customer_ID and snapshot_dates we need features for
    df_base = df_fin.select("Customer_ID", "snapshot_date").distinct()
    
    # ---------------------------------------------------------
    # 1. ASOF Join for Attributes
    # We want the latest attribute record <= base snapshot_date
    # ---------------------------------------------------------
    # Create a union of base dates and attribute dates to define the timeline
    df_timeline = df_base.select("Customer_ID", "snapshot_date").unionByName(
        df_attr.select("Customer_ID", "snapshot_date")
    ).distinct()
    
    # Left join attributes onto the timeline
    df_timeline_attr = df_timeline.join(df_attr, ["Customer_ID", "snapshot_date"], "left")
    
    # Define a window to forward fill the latest non-null attribute
    # ordered by snapshot_date from the beginning of history up to current row
    w_asof = Window.partitionBy("Customer_ID").orderBy("snapshot_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    # Forward fill all columns from df_attr
    attr_cols = [c for c in df_attr.columns if c not in ["Customer_ID", "snapshot_date"]]
    for c in attr_cols:
        df_timeline_attr = df_timeline_attr.withColumn(c, _last(col(c), ignorenulls=True).over(w_asof))
        
    # Now join the forward-filled attributes back to our exact base dates
    df_feat = df_base.join(df_timeline_attr, ["Customer_ID", "snapshot_date"], "inner")
    
    # ---------------------------------------------------------
    # 2. Join Financials (which forms our base)
    # Since financials defined our base dates, we can just do an exact join
    # ---------------------------------------------------------
    df_feat = df_feat.join(df_fin, ["Customer_ID", "snapshot_date"], "left")
    
    if "Outstanding_Debt" in df_feat.columns and "Annual_Income" in df_feat.columns:
        df_feat = df_feat.withColumn("Debt_to_Income", col("Outstanding_Debt") / (col("Annual_Income") + 1))
        
    # ---------------------------------------------------------
    # 3. Dynamic Clickstream Aggregations (Time-windowed)
    # We want sum and avg of all fe_1 to fe_20 over the last 90 days.
    # ---------------------------------------------------------
    df_click_ts = df_click.withColumn("ts", F.unix_timestamp("snapshot_date"))
    days_90 = 90 * 86400
    w_click = Window.partitionBy("Customer_ID").orderBy("ts").rangeBetween(-days_90, 0)
    
    # Prepare the selections
    select_exprs = [col("Customer_ID"), col("snapshot_date"), col("ts")]
    for i in range(1, 21):
        select_exprs.append(col(f"fe_{i}"))
        
    df_click_agg = df_click_ts.select(*select_exprs)
    
    # Dynamically apply rolling sum and avg for all 20 features
    for i in range(1, 21):
        col_name = f"fe_{i}"
        df_click_agg = df_click_agg \
            .withColumn(f"{col_name}_sum_90d", _sum(col_name).over(w_click)) \
            .withColumn(f"{col_name}_avg_90d", _avg(col_name).over(w_click))
            
    df_click_agg = df_click_agg.drop("ts", *[f"fe_{i}" for i in range(1, 21)])
    
    # Because there might be multiple clicks on the same snapshot_date, drop duplicates
    df_click_agg = df_click_agg.dropDuplicates(["Customer_ID", "snapshot_date"])
    
    # ASOF join for clickstream aggregates to our base timeline
    # We use the same forward-fill approach
    df_timeline_click = df_timeline.join(df_click_agg, ["Customer_ID", "snapshot_date"], "left")
    
    agg_cols = [c for c in df_click_agg.columns if c not in ["Customer_ID", "snapshot_date"]]
    for c in agg_cols:
        # Fill forward the latest 90-day aggregation up to the anchor date
        df_timeline_click = df_timeline_click.withColumn(c, _last(col(c), ignorenulls=True).over(w_asof))
        
    # Join to the final feature store
    df_gold = df_feat.join(df_timeline_click, ["Customer_ID", "snapshot_date"], "inner")
    
    # Fill NAs for missing clickstream with 0 (assuming no clicks means 0)
    fill_dict = {c: 0.0 for c in agg_cols}
    df_gold = df_gold.fillna(fill_dict)
    
    out_path = os.path.join(datamart_gold_dir, "gold_feature_store.parquet")
    df_gold.write.mode("overwrite").parquet(out_path)
    print(f"Saved gold feature store to {out_path} with {df_gold.count()} rows.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldTest").getOrCreate()
    process_feature_gold(spark, "datamart/silver", "datamart/gold")
