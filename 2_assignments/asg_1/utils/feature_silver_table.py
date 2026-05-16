import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, trim

def process_feature_silver(spark: SparkSession, datamart_bronze_dir: str, datamart_silver_dir: str):
    """
    Clean Bronze data and write to Silver layer.
    """
    print("--- Processing Feature Silver Tables ---")
    
    # 1. Clickstream
    click_path = os.path.join(datamart_bronze_dir, "bronze_feature_clickstream.parquet")
    if os.path.exists(click_path):
        print("Cleaning Clickstream data...")
        df_click = spark.read.parquet(click_path)
        
        # Cast all fe_ columns to double
        for i in range(1, 21):
            col_name = f"fe_{i}"
            df_click = df_click.withColumn(col_name, col(col_name).cast("double"))
        
        df_click = df_click.withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))
        df_click = df_click.dropna(subset=["Customer_ID", "snapshot_date"])
        df_click = df_click.dropDuplicates(["Customer_ID", "snapshot_date"])
        
        out_path = os.path.join(datamart_silver_dir, "silver_feature_clickstream.parquet")
        df_click.write.mode("overwrite").parquet(out_path)
        print(f"Saved silver clickstream to {out_path}")

    # 2. Attributes
    attr_path = os.path.join(datamart_bronze_dir, "bronze_feature_attributes.parquet")
    if os.path.exists(attr_path):
        print("Cleaning Attributes data...")
        df_attr = spark.read.parquet(attr_path)
        
        df_attr = df_attr.withColumn("Age", col("Age").cast("int"))
        # Clean SSN by removing anything that is not a digit or hyphen
        df_attr = df_attr.withColumn("SSN", regexp_replace(col("SSN"), r"[^\d-]", ""))
        df_attr = df_attr.withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))
        
        df_attr = df_attr.dropna(subset=["Customer_ID", "snapshot_date"])
        df_attr = df_attr.dropDuplicates(["Customer_ID", "snapshot_date"])
        
        out_path = os.path.join(datamart_silver_dir, "silver_feature_attributes.parquet")
        df_attr.write.mode("overwrite").parquet(out_path)
        print(f"Saved silver attributes to {out_path}")

    # 3. Financials
    fin_path = os.path.join(datamart_bronze_dir, "bronze_feature_financials.parquet")
    if os.path.exists(fin_path):
        print("Cleaning Financials data...")
        df_fin = spark.read.parquet(fin_path)
        
        # Strip trailing underscores from Annual_Income and cast to double
        df_fin = df_fin.withColumn("Annual_Income", regexp_replace(col("Annual_Income"), r"_$", "").cast("double"))
        
        # Ensure correct types for numeric columns
        numeric_cols = [
            "Monthly_Inhand_Salary", "Num_Bank_Accounts", "Num_Credit_Card", 
            "Interest_Rate", "Num_of_Loan", "Delay_from_due_date", 
            "Num_of_Delayed_Payment", "Changed_Credit_Limit", "Num_Credit_Inquiries",
            "Outstanding_Debt", "Credit_Utilization_Ratio", "Total_EMI_per_month",
            "Amount_invested_monthly", "Monthly_Balance"
        ]
        
        for c in numeric_cols:
            if c in df_fin.columns:
                # Strip non-numeric chars except dot and minus just to be safe
                df_fin = df_fin.withColumn(c, regexp_replace(col(c), r"[^\d.-]", "").cast("double"))
                
        df_fin = df_fin.withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))
        df_fin = df_fin.dropna(subset=["Customer_ID", "snapshot_date"])
        df_fin = df_fin.dropDuplicates(["Customer_ID", "snapshot_date"])
        
        out_path = os.path.join(datamart_silver_dir, "silver_feature_financials.parquet")
        df_fin.write.mode("overwrite").parquet(out_path)
        print(f"Saved silver financials to {out_path}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverTest").getOrCreate()
    process_feature_silver(spark, "datamart/bronze", "datamart/silver")
