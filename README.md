# ⚡ Energy Consumption Analysis Pipeline

## 🚀 Project Overview

The Energy Consumption Analysis Pipeline is an end-to-end data engineering project designed to ingest, process, validate, transform, and analyze energy-grid data using a modern cloud data platform.

The project processes energy usage, device, weather, traffic, and grid-load data through a layered Bronze → Silver → Gold architecture.

The solution uses Azure Data Lake Storage, Databricks, Delta Lake, dbt, Apache Airflow, Slack monitoring, and GitHub for version control.

The final Gold layer provides dimensional and fact tables that can be used for energy consumption, grid load, household, feeder, substation, operator, and zone analysis.

```text
Source Data
     ↓
Azure Data Lake Storage
     ↓
Bronze Layer
     ↓
Silver Layer
     ↓
Gold Layer
     ↓
Analytics / Dashboards
     ↓
Monitoring & Alerts
```

## 🎯 Project Objectives

* Build an end-to-end energy grid data pipeline.
* Ingest raw energy-related datasets into Azure Data Lake Storage.
* Maintain raw source data in the Bronze layer.
* Clean and standardize data in the Silver layer.
* Apply data-quality and validation rules.
* Build dimensional and fact tables in the Gold layer.
* Implement a Star Schema for analytical workloads.
* Use dbt for SQL-based transformation and modelling.
* Use Apache Airflow for pipeline orchestration.
* Integrate Airflow with dbt Cloud.
* Implement failure handling and Slack notifications.
* Create analytics-ready datasets for energy-grid reporting.
* Maintain the project using Git and GitHub.


## 🛠️ Technology Stack

| **Technology**                   | **Purpose**                                             |
| -------------------------------- | ------------------------------------------------------- |
| **Azure Data Lake Storage Gen2** | Source/raw data storage                                 |
| **Databricks**                   | Data processing and Delta tables                        |
| **Apache Spark / PySpark**       | Distributed data processing                             |
| **Delta Lake**                   | Reliable storage for Bronze, Silver and Gold layers     |
| **Unity Catalog**                | Catalog management and data governance                  |
| **dbt Cloud**                    | SQL transformations and data modelling                  |
| **Apache Airflow**               | Pipeline orchestration and workflow management          |
| **Slack**                        | Pipeline and task failure notifications                 |
| **Git**                          | Version control                                         |
| **GitHub**                       | Source-code repository and collaboration                |
| **Databricks SQL**               | Data exploration, SQL queries, reporting and dashboards |

## 📂 Source Datasets

The Energy Grid pipeline processes multiple energy-related datasets.

| **Dataset**                      | **Purpose**                                              |
| -------------------------------- | -------------------------------------------------------- |
| **energy_usage_stream**          | Household energy consumption and electrical measurements |
| **device_metrics_stream_v2_raw** | Device performance and energy consumption                |
| **weather_source_v2_messy**      | Weather and environmental measurements                   |
| **tariff_metrics_stream_v2**     | Electricity tariff and billing information               |
| **grid_load_stream_messy**       | Grid load, capacity and substation measurements          |

# 🥉 Bronze Layer – Raw Data

The Bronze layer stores the raw source data with minimal transformation.

### Bronze Responsibilities

* Preserve the original source structure.
* Store raw records in Delta format.
* Maintain source-level data.
* Provide a reliable starting point for downstream processing.
* Preserve data for reprocessing and auditing.
* Support downstream Silver transformations.

```text
Azure Data Lake Storage
          │
          ▼
       Bronze
          │
          ├── energy_metrics
          ├── device_metrics
          ├── grid_load
          ├── traffic_metrics
          └── weather
```

# 🥈 Silver Layer – Data Cleaning & Transformation

The Silver layer converts raw Bronze data into clean, standardized and validated datasets.

### Key Activities

* Remove duplicate records.
* Handle NULL values.
* Trim and standardize attributes.
* Convert columns to appropriate data types.
* Validate energy measurements.
* Validate grid measurements.
* Standardize categorical values.
* Validate timestamps.
* Apply business rules.
* Generate data-quality indicators.

```text
azuredb_cen.silver
│
├── energy_metrics
├── silver_device_metrics
├── silver_grid
├── silver_traffic
└── silver_weather
```

# 🥇 Gold Layer – Dimensional Model

The Gold layer provides business-ready dimensional and fact tables for energy-grid analytics.

The model follows a Star Schema.

```text
azuredb_cen.gold
│
├── dim_household
├── dim_feeder
├── dim_operator
├── dim_substation
├── dim_zone
└── fact_grid_load
```

### Fact Grain

The `fact_grid_load` table represents the grain of a grid-load measurement/record at the source measurement level.

The dimension tables provide descriptive attributes for households, feeders, operators, substations and zones.

### Dimension Tables

#### dim_household

Purpose: Stores descriptive attributes related to energy consumers/households.

Important attributes:

* `household_id`
* `region_name`
* `city_name`
* `meter_type`
* `customer_category`
* `grid_zone`

#### dim_feeder

Purpose: Stores feeder-related information used for grid distribution analysis.

#### dim_operator

Purpose: Stores grid operator information.

#### dim_substation

Purpose: Stores substation-related information.

#### dim_zone

Purpose: Stores distribution/grid-zone information.

## FACT_GRID_LOAD

The `fact_grid_load` table contains measurable grid-load metrics used for analytical reporting.

### Typical Measures

* Grid load
* Grid capacity
* Transformer load
* Line loss
* Load variation
* Frequency variation
* Demand forecast
* Reserve margin

## Source-to-Target Mapping

```text
Bronze
energy_metrics
      │
      ▼
Silver
energy_metrics
      │
      ▼
Gold
dim_household
      │
      └───────────────┐
                      ▼
                 fact_grid_load
```

### Transformation Rules

* Trim string columns.
* Standardize categorical values.
* Convert numeric fields to appropriate data types.
* Validate electrical measurements.
* Handle NULL values.
* Remove duplicates.
* Validate timestamps.
* Standardize region and zone attributes.
* Map source identifiers to dimensional attributes.

# 🧪 Data Quality & Testing

Data quality is implemented across the Bronze, Silver and Gold layers.

### Validation Areas

* Schema validation
* NULL validation
* Duplicate validation
* Data type validation
* Range validation
* Timestamp validation
* Referential validation
* Business-rule validation
* Row-count validation
* Transformation validation

# 🧰 dbt Implementation

dbt is used for SQL-based transformation and dimensional modelling.

The dbt project reads Bronze tables from Databricks and creates Silver and Gold models.

### Source

```text
azuredb_cen.bronze
```

### Silver

```text
azuredb_cen.silver
```

### Gold

```text
azuredb_cen.gold
```

### dbt Project Structure

```text
dbt/
│
├── models/
│   ├── silver/
│   │   ├── energy_metrics.sql
│   │   ├── silver_device.sql
│   │   ├── silver_grid.sql
│   │   ├── silver_tariff.sql
│   │   └── silver_weather.sql
│   │
│   └── gold/
│       ├── dim_household.sql
│       ├── dim_feeder.sql
│       ├── dim_operator.sql
│       ├── dim_substation.sql
│       ├── dim_zone.sql
│       └── fact_grid_load.sql
│
├── macros/
│   └── generate_schema_name.sql
│
├── tests/
│
├── sources.yml
└── dbt_project.yml
```

# 🔄 Pipeline Orchestration

Apache Airflow is used to orchestrate the dbt Cloud transformation pipeline.

Airflow triggers the dbt Cloud deployment job, waits for its completion, and receives the final execution status.

```text
Airflow
   │
   ▼
DbtCloudRunJobOperator
   │
   ▼
dbt Cloud
   │
   ▼
ENERGY_GRID_AIRFLOW_JOB
   │
   ▼
dbt build
   │
   ▼
Databricks
```

# 🚨 Failure Handling & Slack Alerts

The pipeline implements centralized task-failure handling in Airflow.

When an Airflow task fails, the failure callback sends a notification to Slack.

The Slack notification contains:

* DAG name
* Task name
* Execution time
* Failure status
* Instructions to check Airflow/dbt Cloud logs

```text
Airflow Task
     │
     ├── SUCCESS
     │      │
     │      ▼
     │   Continue
     │
     └── FAILURE
            │
            ▼
      failure_handler.py
            │
            ▼
        Slack Webhook
            │
            ▼
       🚨 Alert
```

# 📊 Monitoring

The pipeline provides monitoring at multiple levels.

### Airflow

* DAG execution
* Task status
* Task failures
* Execution logs

### dbt Cloud

* dbt build status
* Model execution
* Test execution
* Transformation logs

### Databricks

* Job execution
* Table creation
* Transformation execution
* Data validation

### Slack

* Task failure notifications

# 🔐 Version Control & Security

Git and GitHub are used for source-code version control.

The repository contains:

* Airflow DAGs
* Failure handlers
* dbt models
* dbt macros
* Tests
* Documentation
* Architecture diagrams
* Configuration templates

Sensitive credentials are not committed to GitHub.

The following must remain outside source control:

* Databricks PAT
* dbt Cloud API token
* Slack webhook URL
* Client secrets
* Database passwords

# 📁 Project Structure

```text
energy-comsumption-analysis-pepelines/
│
├── Alerts/
│   └── Slack_Failure_Alert.md
│
├── Dashboards/
│   └── Energy_Grid_Dashboard.pdf
│
├── Datasets/
│   └── Source/
│       ├── energy_usage_stream.csv
│       ├── device_metrics_stream_v2_raw.csv
│       ├── weather_source_v2_messy.csv
│       ├── tariff_metrics_stream_v2.csv
│       └── grid_load_stream_messy.csv
│
├── Design/
│   ├── high_level_architecture.png
│   ├── low_level_design.png
│   ├── data_model_diagram.png
│   └── data_lake_architecture.png
│
├── Development/
│   ├── Bronze/
│   ├── Silver/
│   └── Gold/
│       ├── Dimension Tables/
│       └── Fact Tables/
│
├── dbt/
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── tests/
│   ├── sources.yml
│   └── dbt_project.yml
│
├── Airflow/
│   ├── dags/
│   │   ├── energy_grid_dbt_pipeline.py
│   │   └── failure_handler.py
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
│
├── Tests/
│   ├── Test_Bronze.sql
│   ├── Test_Silver.sql
│   └── Test_Gold.sql
│
├── README.md
└── .gitignore
```

# 📌 Key Outcomes

* Implemented an end-to-end Energy Grid data engineering pipeline.
* Implemented Bronze, Silver and Gold architecture.
* Processed energy, device, weather, traffic and grid-load datasets.
* Built standardized Silver Delta tables.
* Implemented Gold dimensional and fact models.
* Implemented a Star Schema for analytical workloads.
* Implemented dbt Cloud transformations.
* Successfully executed dbt build against Databricks.
* Implemented Apache Airflow orchestration.
* Successfully integrated Airflow with dbt Cloud.
* Implemented centralized Airflow failure handling.
* Integrated Slack failure notifications.
* Implemented Git/GitHub version control.
* Created an analytics-ready Energy Grid data platform.

# 🔮 Future Enhancements

* Automated CI/CD using GitHub Actions.
* Automated dbt documentation generation.
* Automated data-quality reporting.
* Enhanced Databricks dashboarding.
* Pipeline SLA monitoring.
* Centralized audit logging.
* Advanced grid-load analytics.
* Real-time streaming ingestion.
* Automated anomaly detection.
* Advanced energy-consumption forecasting.
* Production-grade Airflow scheduling.
