Links:
[Dashboard](https://lookerstudio.google.com/reporting/cd43d09c-9ca3-4696-91c3-1d033010ff0e)
[Google Sheets Monthly Metric sample](https://docs.google.com/spreadsheets/d/1-Fg7Xy45WfHb70J68VlHk8YVlEIfwqC1-JC1A4EZ3uE/edit?usp=sharing)

🚀 Overview

Subscription businesses need to understand whether growth is being driven by customer acquisition, renewals, or pricing. They also need visibility into churn and revenue trends across subscription plans.
This project was built to model that workflow end to end and produce monthly metrics that can support reporting and decision-making.

It follows a classic production-grade data engineering pattern:

        Data Generation → Cloud Storage (Staging) → Data Warehouse (BigQuery)

The pipeline handles high-volume data efficiently using chunked processing and cloud-native services, ensuring memory stability and scalability.

🏗️ Architecture

The following diagram illustrates the end-to-end flow of data through the system:

    A[Python Script] --> B[Generate Synthetic Data]
    
    B --> C[Chunk into CSV Files]
    
    C --> D[Upload to GCS Staging]
    
    D --> E[Load into BigQuery]
    
    E --> F[(Partitioned + Clustered Table)]
⚙️ What the Pipeline DoesScalable Generation: 

        * Creates synthetic subscription data for millions of customers.

* Memory Management: 

        * Splits data into chunked CSV files to prevent OOM (Out of Memory) errors.

* Staging Layer: 

        * Uploads files to Google Cloud Storage (GCS) before ingestion.

* Automated Ingestion:

        * Loads data into BigQuery using batch load jobs.Idempotency & Safety: 

        * Automatically creates tables if they don't exist.

        * Appends data for incremental ingestion.

        * Cleans up temporary local files after successful processing.

💡 Why This Project Matters

* This project demonstrates core Data Engineering competencies:

        * Batch Processing -- Efficiently handling large datasets in discrete chunks.

        * Cloud Staging -- Using GCS as a landing zone/buffer for raw data.

        * Warehouse Optimization -- Implementing Partitioning (by created_at) and Clustering (by subscription_name) for query performance.

        * Observability -- Structured logging to track pipeline health.

        * Infrastructure -- Environment-based configuration and resource automation.
  
🧰 Tech Stack
* Language: Python 3.x
* Storage: Google Cloud Storage (GCS)
* Warehouse: Google BigQueryLibraries:

        google-cloud-storage, google-cloud-bigquery, pandas

🔧 Key Engineering FeaturesEnvironment-based configuration: 
* Manage variables dynamically.

Reusable GCP clients: 
* Optimized connection handling.

Safe file cleanup: 
* Uses try/finally blocks to ensure local disk space is reclaimed.

Optimized Schema:
* Partitioning: By created_at to reduce data scanned during time-based queries.

Clustering: 
* By subscription_name and customer_id for faster filtering.

☁️ Infrastructure Setup

1. Create Cloud Storage Bucket
        * This bucket acts as a staging layer for batch files before ingestion.

        gcloud storage buckets create gs://YOUR_BUCKET_NAME --location=US
      
3. Create BigQuery DatasetThis dataset will contain the final analytical tables.

        bq mk --dataset YOUR_PROJECT_ID:subscriptions
      
4. Verify Resources

        gsutil ls
           bq ls

▶️ How to Run the Pipeline
  1. Install Dependencies

                pip install google-cloud-storage google-cloud-bigquery

  3. Authenticate with Google Cloud

                gcloud auth application-default login

                gcloud config set project YOUR_PROJECT_ID

  4. Run a Test Version (Recommended)
   
    * Run a smaller dataset first to validate the pipeline logic:
        * Windows (CMD)
                set NUM_CUSTOMERS=10000
                set MAX_ROWS_PER_FILE=50000
                python random_subscription_data.py
        
        * PowerShellPowerShell
                $env:NUM_CUSTOMERS="10000"
                $env:MAX_ROWS_PER_FILE="50000"
                python random_subscription_data.py

  4. Run Full Pipeline
     
    python random_subscription_data.py

🔍 Validate Results in BigQueryRun these queries in the BigQuery console to verify ingestion:

    * Check Total Row Count:
        SQLSELECT COUNT(*) FROM `YOUR_PROJECT_ID.subscriptions.subscriptions`
        
    * Preview Data:
        SQLSELECT * FROM `YOUR_PROJECT_ID.subscriptions.subscriptions` LIMIT 100
