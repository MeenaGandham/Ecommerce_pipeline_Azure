# Ecommerce Datapipeline with Azure and Databricks
## Overview
This project is a real-time data pipeline designed to ingest, process, store, and visualize e-commerce data efficiently using Azure Cloud services. The pipeline ensures scalability, automation, and high performance for data-driven decision-making.

## Project Objective
The goal of this project is to automate the processing of e-commerce sales data from multiple sources (such as transactional databases, APIs, and CSV files), transform it into structured datasets, and store it in a cloud-based data warehouse (Snowflake/Azure Synapse) for analytics and business intelligence.
This project follows a modern data pipeline architecture, ensuring:
- Efficient data ingestion using Azure Data Factory (ADF)
- Robust ETL processing using Azure Databricks (Apache Spark)
- Organized data storage using Azure Data Lake (ADLS)
- Optimized querying using Snowflake/Azure Synapse
- Data visualization & analytics using Power BI or Tableau

## Architecture Overview
![Project Architecture](resources/architecture.png)

### Data Sources (Raw Data Ingestion)
E-commerce platform sales data is collected from CSV files stored in Blob Storage. Data is structured (CSV, JSON) and semi-structured (logs, XML).

### Data Ingestion Using Azure Data Factory (ADF)
Azure Data Factory (ADF) automates data extraction from sources. The pipeline efficiently manages the ingestion, transformation, and storage of e-commerce data. The process begins with Copy Data activities, where raw data from various sources, such as users, sellers, buyers, and countries, is extracted and stored in Azure Data Lake Storage (ADLS). These operations are orchestrated using event-based and scheduled triggers, ensuring that new data is processed as soon as it arrives or at predefined intervals.Data is stored in Azure Data Lake Storage (ADLS) in Bronze Layer (Raw Data).
Triggers and scheduling enable automatic ingestion every hour or day.
!Bronze Layer = unprocessed raw data!

### Data Processing & Transformation Using Azure Databricks (ETL)
Azure Databricks (Apache Spark) processes the raw data:
- Cleans missing values, handles duplicates.
- Converts timestamps, normalizes customer/product data.
- Joins tables to enrich sales records with customer & product details.
- Transformed data is stored back in Azure Data Lake (Silver Layer).
!Silver Layer = Cleaned and enriched data!
- Read cleaned data from the Silver Layer; aggregate, join, and optimize for business insights.
- Transformed data is stored back in Azure Data Lake (Gold Layer).
!Gold Layer = aggregated and denormalized data!

### SQL-Based Analysis & Reporting in Databricks
After data is processed and stored in the Gold Layer, Databricks SQL is used for analytics. SQL queries extract insights directly from Delta Tables in Databricks. Databricks SQL Dashboards provide real-time business intelligence.




