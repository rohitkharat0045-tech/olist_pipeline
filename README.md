# 🛒 Olist E-Commerce End-to-End Data Engineering Pipeline

An end-to-end **Data Engineering project** built using the Brazilian **Olist E-Commerce dataset**. This project demonstrates how raw e-commerce data can be ingested, validated, transformed, orchestrated, and processed through a scalable data pipeline.

The pipeline uses **Python, Apache Airflow, PySpark, Google Cloud Storage, and BigQuery** to build an automated and production-style data engineering workflow.

---

## 📌 Project Overview

The goal of this project is to build a complete data engineering pipeline for analyzing Brazilian e-commerce data.

The pipeline processes multiple Olist datasets containing information about:

* Customers
* Sellers
* Orders
* Order Items
* Payments
* Reviews
* Products
* Geolocation
* Product Category Translation

The pipeline performs data ingestion, data quality checks, transformations, joins, and reporting to create clean and analytics-ready datasets.

---

## 🏗️ Architecture

```text
                    ┌──────────────────────┐
                    │   Olist CSV Dataset  │
                    │      (Raw Data)      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Data Ingestion      │
                    │   Python / Pandas     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Data Quality       │
                    │      Checks          │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Apache Airflow     │
                    │    Orchestration     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      PySpark         │
                    │  Data Transformation │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  Master Data Model   │
                    │   & Cleaned Data     │
                    └──────────┬───────────┘
                               │
                    ┌──────────┴───────────┐
                    ▼                      ▼
          ┌─────────────────┐    ┌─────────────────┐
          │ Google Cloud    │    │    Reports /    │
          │ Storage (GCS)   │    │   Analytics     │
          └────────┬────────┘    └─────────────────┘
                   │
                   ▼
          ┌─────────────────┐
          │    BigQuery     │
          │ Data Warehouse  │
          └─────────────────┘
```

---

## 🚀 Technologies Used

| Technology               | Purpose                                            |
| ------------------------ | -------------------------------------------------- |
| **Python**               | Data ingestion, validation, and pipeline utilities |
| **Pandas**               | Raw data profiling and preprocessing               |
| **PySpark**              | Distributed data transformation and processing     |
| **Apache Airflow**       | Workflow orchestration and scheduling              |
| **Google Cloud Storage** | Cloud-based data storage                           |
| **BigQuery**             | Cloud data warehouse and analytics                 |
| **SQL**                  | Data analysis and querying                         |
| **Bash**                 | Pipeline automation and shell scripts              |
| **WSL2 / Ubuntu**        | Linux-based development environment                |
| **Git & GitHub**         | Version control and project management             |

---

## 📂 Project Structure

```text
olist_pipeline/
│
├── dags/
│   └── olist_pipeline_dag.py
│
├── raw/
│   └── *.csv
│
├── processed/
│   └── cleaned_data/
│
├── reports/
│   └── generated_reports/
│
├── logs/
│   └── pipeline_logs/
│
├── notebooks/
│   └── exploratory_analysis.ipynb
│
├── sql/
│   └── analytical_queries.sql
│
├── scripts/
│   ├── profile_raw_data.sh
│   └── run_pipeline.sh
│
├── src/
│   ├── ingestion/
│   │   └── profiler.py
│   │
│   ├── transform/
│   │   └── spark_pipeline.py
│   │
│   └── utils/
│       └── logger.py
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🔄 Pipeline Workflow

The pipeline follows these major stages:

### 1. Data Ingestion

The Olist CSV datasets are loaded from the raw data source and prepared for processing.

The ingestion layer is responsible for:

* Reading raw CSV files
* Validating file availability
* Inspecting schemas
* Profiling columns
* Checking row counts
* Identifying missing values

---

### 2. Data Quality Checks

Before transformation, the pipeline performs data quality checks to ensure that the input data is valid.

Checks include:

* File existence
* Row counts
* Column counts
* Missing values
* Duplicate records
* Data type validation
* Null value analysis

---

### 3. Apache Airflow Orchestration

Apache Airflow is used to automate and orchestrate the pipeline.

The main DAG is:

```text
olist_pipeline
```

The DAG manages the execution order of pipeline tasks and allows the workflow to be monitored through the Airflow UI.

Example workflow:

```text
Start
  │
  ▼
Profile Raw Data
  │
  ▼
Run Data Quality Checks
  │
  ▼
Run PySpark Transformation
  │
  ▼
Build Master Dataset
  │
  ▼
Generate Reports
  │
  ▼
End
```

The pipeline is configured to run automatically based on the Airflow schedule.

---

### 4. PySpark Transformation

PySpark is used to process and transform the Olist datasets.

The transformation process includes:

* Reading multiple CSV datasets
* Cleaning column names
* Handling missing values
* Removing duplicate records
* Data type conversions
* Joining related datasets
* Optimizing joins using broadcast operations where appropriate
* Building a master dataset
* Caching frequently used datasets
* Registering temporary SQL views

---

### 5. Master Dataset

The transformed datasets are joined to create an analytics-ready master dataset.

The master dataset combines key information such as:

* Order details
* Customer information
* Seller information
* Product information
* Payment information
* Review information
* Order item details

This provides a unified dataset for downstream analytics.

---

### 6. Reporting and Analytics

The final processed data can be used to generate business insights such as:

* Total revenue
* Number of orders
* Average order value
* Top-selling products
* Best-performing product categories
* Customer distribution
* Seller performance
* Payment method analysis
* Delivery performance
* Review score analysis
* Order trends over time

---

## 📊 Olist Dataset

The project is based on the **Brazilian E-Commerce Public Dataset by Olist**.

The dataset contains approximately:

* 99K orders
* 112K order items
* 99K customers
* Multiple related datasets covering the e-commerce lifecycle

The datasets are connected using keys such as:

```text
order_id
customer_id
customer_unique_id
product_id
seller_id
```

---

## ⚙️ Local Setup

### Prerequisites

Make sure the following are installed:

* Python 3.12+
* Java 17
* Apache Spark / PySpark
* Apache Airflow
* WSL2 with Ubuntu
* Git

For cloud functionality:

* Google Cloud account
* Google Cloud Storage bucket
* BigQuery dataset
* Google Cloud credentials

---

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/olist_pipeline.git

cd olist_pipeline
```

---

### 2. Create a Python Virtual Environment

```bash
python3 -m venv venv_linux
```

Activate the environment:

```bash
source venv_linux/bin/activate
```

---

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Configure Environment Variables

Create a `.env` or configuration file according to your environment.

Example:

```env
APP_ENV=dev

GCS_BUCKET_NAME=your-bucket-name
GCP_PROJECT_ID=your-project-id
BIGQUERY_DATASET=your-dataset
```

> Never commit service account keys, passwords, API keys, or other secrets to GitHub.

---

## ▶️ Running the Pipeline

### Run Data Profiling

```bash
bash scripts/profile_raw_data.sh
```

---

### Run the PySpark Pipeline

```bash
python3 -m src.transform.spark_pipeline
```

---

### Run the Complete Pipeline

```bash
bash scripts/run_pipeline.sh
```

---

## 🌬️ Running with Apache Airflow

Activate the Airflow environment:

```bash
source ~/airflow-env/bin/activate
```

Check the Airflow DAG folder:

```bash
airflow config get-value core dags_folder
```

List available DAGs:

```bash
airflow dags list
```

Check for DAG import errors:

```bash
airflow dags list-import-errors
```

Start the Airflow webserver:

```bash
airflow webserver -p 8080
```

Start the scheduler in another terminal:

```bash
airflow scheduler
```

Open the Airflow UI:

```text
http://localhost:8080
```

Then search for:

```text
olist_pipeline
```

You can trigger the pipeline manually from the Airflow UI or using the CLI:

```bash
airflow dags trigger olist_pipeline
```

---

## ☁️ Google Cloud Integration

The pipeline can be integrated with Google Cloud services for scalable data engineering workflows.

### Google Cloud Storage

Used as the cloud storage layer for raw and processed datasets.

```text
Local / Source Data
        │
        ▼
Google Cloud Storage
        │
        ▼
PySpark Processing
```

### BigQuery

BigQuery can be used as the analytical data warehouse.

```text
PySpark
   │
   ▼
Processed Data
   │
   ▼
BigQuery
   │
   ▼
SQL Analytics
```

---

## 📈 Key Data Engineering Concepts Demonstrated

This project demonstrates practical knowledge of:

* ETL / ELT pipelines
* Batch data processing
* Data ingestion
* Data profiling
* Data quality validation
* Data cleaning
* Distributed processing
* PySpark transformations
* Broadcast joins
* Dataset caching
* Data orchestration
* Airflow DAG development
* Pipeline scheduling
* Cloud storage
* Data warehousing
* BigQuery analytics
* Linux / WSL development
* Shell scripting
* Logging and monitoring
* Git version control

---

## 🎯 Project Goals

The main objectives of this project are:

1. Build a complete end-to-end data pipeline.
2. Automate pipeline execution using Apache Airflow.
3. Process large datasets using PySpark.
4. Implement data quality checks.
5. Create clean and analytics-ready datasets.
6. Integrate cloud storage and data warehouse technologies.
7. Demonstrate production-style data engineering practices.

---

## 🔮 Future Improvements

Possible future improvements include:

* Add Docker and Docker Compose
* Deploy Airflow to the cloud
* Add automated unit and integration tests
* Add CI/CD using GitHub Actions
* Add Great Expectations for advanced data quality
* Implement incremental data processing
* Add data lineage
* Add monitoring and alerting
* Create Power BI / Looker Studio dashboards
* Implement a modern data lakehouse architecture

---

## 👨‍💻 Author

**Rohit Kharat**

Data Engineering Project | Olist E-Commerce End-to-End Pipeline

---

## ⭐ Acknowledgements

* Brazilian E-Commerce Public Dataset by Olist
* Kaggle
* Apache Airflow
* Apache Spark
* Google Cloud Platform

---

## 📄 License

This project is intended for educational and portfolio purposes.
