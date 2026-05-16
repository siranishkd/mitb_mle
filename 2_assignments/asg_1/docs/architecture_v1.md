# ETL Architecture for Assignment 1

The proposed data pipeline follows the **Medallion Architecture (Bronze -> Silver -> Gold)**, ensuring separation of concerns, data quality, and reusability. It is designed to be compatible with a downstream binary classification ML model while carefully avoiding data leakage.

## Architecture Diagram

```mermaid
flowchart TD
    subgraph Raw Data
        R1[(feature_clickstream.csv)]
        R2[(features_attributes.csv)]
        R3[(features_financials.csv)]
        R4[(lms_loan_daily.csv)]
    end

    subgraph Bronze Layer [Bronze: Raw Parquet]
        B1[(Bronze Clickstream)]
        B2[(Bronze Attributes)]
        B3[(Bronze Financials)]
        B4[(Bronze Loans)]
    end

    subgraph Silver Layer [Silver: Cleaned & Typed]
        S1[(Silver Clickstream<br>Cleaned, Typed)]
        S2[(Silver Attributes<br>Imputed, Cleaned)]
        S3[(Silver Financials<br>Cleaned)]
        S4[(Silver Loans<br>Deduplicated)]
    end

    subgraph Gold Layer [Gold: Business Level]
        G1[(Gold Feature Store<br>Time-Aggregated Features)]
        G2[(Gold Label Store<br>Default Flags)]
    end

    subgraph Final Model Input [Downstream]
        M([ML Training Dataset])
    end

    R1 --> |Ingest| B1
    R2 --> |Ingest| B2
    R3 --> |Ingest| B3
    R4 --> |Ingest| B4

    B1 --> |Cast Types & Validate| S1
    B2 --> |Handle Nulls| S2
    B3 --> |Format Corrections| S3
    B4 --> |Clean Labels| S4

    S1 --> |Rolling Windows / Aggregations| G1
    S2 --> |Latest State Join| G1
    S3 --> |Financial Ratios| G1

    S4 --> |Derive Loan Default / Target| G2

    G1 --> |Point-in-Time Join<br>No Leakage| M
    G2 --> |Labels| M

    classDef raw fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef bronze fill:#cd7f32,stroke:#333,stroke-width:2px,color:#fff;
    classDef silver fill:#c0c0c0,stroke:#333,stroke-width:2px;
    classDef gold fill:#ffd700,stroke:#333,stroke-width:2px;
    classDef model fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff;

    class R1,R2,R3,R4 raw;
    class B1,B2,B3,B4 bronze;
    class S1,S2,S3,S4 silver;
    class G1,G2 gold;
    class M model;
```

## Layer Definitions

1. **Bronze Layer:** Ingest raw CSVs as Parquet files with minimal transformations. This provides a resilient and typed historical archive of the raw data.
2. **Silver Layer:** Apply data cleaning rules. Standardize formats, resolve missing values where appropriate, drop explicit duplicates, and prepare individual entities before feature construction.
3. **Gold Layer:** Perform business-level aggregations and feature engineering. Calculate variables such as `last_30_days_clicks` or `debt_to_income_ratio`.
   * **Important:** Strict point-in-time constraints must be enforced during these joins and aggregations to prevent **Data Leakage** (i.e. we cannot use data that would only be available *after* the loan's outcome is decided).
