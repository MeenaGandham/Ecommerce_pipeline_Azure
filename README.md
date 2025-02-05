# 🚀 Azure E-Commerce Data Pipeline

This project implements an end-to-end **E-commerce Data Pipeline** using **Azure Storage, Azure Data Factory (ADF), Databricks, and Power BI**. The pipeline ingests, processes, and analyzes user, seller, buyer, and country data efficiently using **bronze, silver, and gold layers** (**medallion architecture**) for data transformation. 
![Architecture Overview](resources/archictecture.png)

## 📂 Data Sources and Structure

The pipeline handles four key datasets:

- **Sellers, Buyers, and Countries**: These are **static** tables with minimal updates.
- **Users**: **Dynamic** tables that get generated frequently and is **partitioned into 5 chunks** before processing using ![ipynb][(Chunk_data.ipynb).
The raw data is stored in **Azure Data Lake Storage (ADLS)** in a **Landing Zone 1**.

## 📤 Data Ingestion with Azure Data Factory (ADF)

### 1️⃣ **Data Movement Pipelines**
- **Sellers, Buyers, and Countries**: A **single ADF pipeline** extracts CSV files, converts them to **Parquet**, and stores them in **Landing Zone 2**.
![ADF Pipelines1](resources/ADF1.jpeg)
- **Users Data**: A **separate ADF pipeline** processes user chunks since it updates frequently.
![ADF Pipelines2](resources/ADF2.jpeg)

### 2️⃣ **Automated Triggers**
- **Sellers, Buyers, Countries** → **Scheduled** trigger for periodic execution.
- **Users Data** → **Event-based** trigger, executing when a new file is uploaded.

The transformed data is stored in **Landing Zone 2**:

## 🛠 Data Processing with Databricks

### 1️⃣ **Trigger-Driven Bronze-Silver-Gold Pipeline**
- A **trigger in ADF** is set up to **initiate the Bronze-Silver-Gold pipeline whenever new data is added to Landing Zone 2**.
- The data pipeline follows **medallion architecture**:

  - **Bronze Layer** → Raw Parquet files stored as **Delta Tables**.
  - **Silver Layer** → Cleaned and structured tables.
  - **Gold Layer** → Aggregated and transformed data for analysis.

🔹 *Bronze → Silver → Gold Pipeline*
![Databricks Pipeline](ADF4.png)

### 2️⃣ **Gold Layer (Final Aggregated Data)**
- The **final Gold Table** is a **combination of all the tables** (Users, Sellers, Buyers, Countries) optimized for analytics.

🔹 *Databricks Gold Layer Processing*
![Databricks Gold Processing](AZ1.jpeg)

---

## 📊 Data Analysis & Visualization in Power BI

- **SQL transformations** were performed in Databricks to **generate analytical insights**.
- **Gold tables** were used for **business intelligence reporting** in **Power BI**.
- **Created an interactive Power BI dashboard** to visualize user behavior, trends, and other insights.

🔹 *Final Power BI Dashboard*
![Power BI Dashboard](Ecommerce_d.jpeg)
![Power BI Dashboard2](ecommerce_d2.jpeg)
You can download the dashboard here : ![dashboard](ecommerce.pbix)


---

## 🎯 Key Features
✔ **Automated data ingestion** with Azure Data Factory  
✔ **Trigger-based Bronze-Silver-Gold execution**  
✔ **Delta Lake storage optimization** using Bronze, Silver, and Gold layers  
✔ **Databricks-powered ETL** for structured transformations  
✔ **Power BI analytics** for business insights  

---

## 🚀 Technologies Used
- **Azure Data Factory (ADF)** - Data ingestion and movement  
- **Azure Data Lake Storage (ADLS)** - Data storage and structuring  
- **Databricks(Apache Spark) + Delta Lake** - ETL processing and storage  
- **Power BI** - Data visualization
- **Azure Data Lake (ADLS)** - Organized data storage

