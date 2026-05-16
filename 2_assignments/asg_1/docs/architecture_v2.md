# ETL Architecture for Assignment 1 (v2.0)

This architecture updates v1.0 by implementing robust "As-Of" (ASOF) Point-in-Time joins. It ensures that features from disparate update cadences (daily clickstream vs. monthly financials) are safely merged without causing Data Leakage or NULL dropping.

## Architecture Diagram

```mermaid
flowchart TD
    subgraph Raw Data
        R1[(feature_clickstream.csv)]
        R2[(features_attributes.csv)]
        R3[(features_financials.csv)]
    end

    subgraph Bronze Layer [Bronze: Raw Parquet]
        B1[(Bronze Clickstream)]
        B2[(Bronze Attributes)]
        B3[(Bronze Financials)]
    end

    subgraph Silver Layer [Silver: Data Quality & Typed]
        S1[(Silver Clickstream<br>Cleaned, Typed, Drop NA)]
        S2[(Silver Attributes<br>Imputed, Cleaned, Drop NA)]
        S3[(Silver Financials<br>Cleaned, Drop NA)]
    end

    subgraph Gold Layer [Gold: Point-in-Time Feature Store]
        G1[(Gold Feature Store<br>ASOF Forward-Filled Features)]
    end

    subgraph Final Model Input [Downstream ML Pipeline]
        L[(Lab 2: Label Store)]
        M([ML Training Dataset])
    end

    R1 --> |Ingest| B1
    R2 --> |Ingest| B2
    R3 --> |Ingest| B3

    B1 --> |Cast Types & Validate| S1
    B2 --> |Handle Nulls & Format| S2
    B3 --> |Format Corrections| S3

    S1 --> |90-Day Rolling Window<br>All fe_1 to fe_20| G1
    S2 --> |ASOF Join<br>Latest State Forward Fill| G1
    S3 --> |ASOF Join<br>Financial Ratios| G1

    G1 --> |Join on Customer_ID + snapshot_date| M
    L --> |Labels| M

    classDef raw fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef bronze fill:#cd7f32,stroke:#333,stroke-width:2px,color:#fff;
    classDef silver fill:#c0c0c0,stroke:#333,stroke-width:2px;
    classDef gold fill:#ffd700,stroke:#333,stroke-width:2px;
    classDef model fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff;

    class R1,R2,R3 raw;
    class B1,B2,B3 bronze;
    class S1,S2,S3 silver;
    class G1 gold;
    class M model;
```
