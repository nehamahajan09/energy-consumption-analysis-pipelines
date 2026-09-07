Energy Consumption Analysis Pipeline
Project Overview

The Energy Consumption Analysis Pipeline is a scalable data engineering solution designed to ingest, clean, transform, and analyze energy consumption data from devices, grids, tariffs, and weather sources.

The project uses Azure Databricks, PySpark, Delta Lake, and Medallion Architecture to build an end-to-end data pipeline. Raw energy data is processed through Bronze, Silver, and Gold layers, producing clean and business-ready datasets for energy monitoring, consumption analysis, efficiency analysis, and dashboard reporting.

The pipeline ensures:

Reliable ingestion of energy-related data
Data cleansing and validation
Removal of duplicate and invalid records
Standardized data types and formats
Energy consumption and efficiency analysis
Business-ready Gold-layer datasets
Data quality testing using Pytest
Monitoring and alerting for pipeline failures

Monitoring and alerting for pipeline failures
Project Objective

The main objectives of the Energy Consumption Analysis Pipeline are:

Build a scalable energy data processing pipeline using Azure Databricks.
Ingest energy data from multiple sources.
Store data using Delta Lake and Medallion Architecture.
Clean and standardize raw data in the Silver layer.
Handle null, duplicate, invalid, and inconsistent records.
Analyze energy consumption across devices, grids, households, zones, and time periods.
Calculate energy efficiency and power-consumption metrics.
Build a Gold-layer star schema for analytics.
Create dashboards for energy consumption and operational insights.
Implement Pytest-based data quality validation.
Monitor pipeline execution and generate alerts for failures.

Dataset
Dataset Source

The project uses energy-related datasets representing data collected from smart devices, electrical grids, tariffs, and weather conditions.

The datasets simulate a real-world smart energy management system.

Datasets Used
Device Data → Device category, brand, model, power, voltage, current, temperature
Energy Metrics → Energy consumption, power usage, load and efficiency
Grid Data → Grid/substation information, load and grid conditions
Tariff Data → Electricity tariff and pricing information
Weather Data → Temperature, humidity, weather conditions and related factor

Technologies Used
| Technology                           | Purpose                           |
| ------------------------------------ | --------------------------------- |
| **Python**                           | Data processing and scripting     |
| **PySpark**                          | Distributed data processing       |
| **Azure Databricks**                 | Data engineering and analytics    |
| **Delta Lake**                       | Reliable data storage             |
| **Azure Data Lake Storage (ADLS)**   | Data lake storage                 |
| **SQL**                              | Data querying and validation      |
| **Pytest**                           | Data quality and pipeline testing |
| **Apache Airflow**                   | Pipeline orchestration            |
| **Slack**                            | Alerts and notifications          |
| **Power BI / Databricks Dashboards** | Data visualization and reporting  |


ETL Pipeline Design
Bronze Layer — Raw Data
Purpose

The Bronze layer stores the raw data as received from the source systems.

It preserves the original data and provides data lineage and traceability.

Tables
azuredb_cen.bronze.bronze_device
azuredb_cen.bronze.bronze_energy_metrics
azuredb_cen.bronze.bronze_grid
azuredb_cen.bronze.bronze_tariff
azuredb_cen.bronze.bronze_weather
Operations
Ingest raw energy datasets.
Store source data in Delta format.
Preserve original records.
Perform basic schema validation.
Maintain data lineage.
Store data in Azure Data Lake/Databricks.

Silver Layer — Cleaned Data
Purpose

The Silver layer contains cleaned, validated, and standardized energy data.

It prepares the data for analytical processing and Gold-layer transformations.

Tables
azuredb_cen.silver.silver_device
azuredb_cen.silver.silver_energy_metrics
azuredb_cen.silver.silver_grid
azuredb_cen.silver.silver_tariff
azuredb_cen.silver.silver_weather
Transformations

The Silver layer performs:

Remove duplicate records.
Handle missing/null values.
Convert columns to correct data types.
Standardize column names.
Validate numeric values.
Remove negative energy/power values.
Validate device measurements.
Standardize categorical values.
Validate temperature and weather information.
Perform data quality checks.


Gold Layer — Analytics Data
Purpose

The Gold layer contains business-ready datasets used for reporting, dashboards, and decision-making.

The project follows a Star Schema design.

Gold Tables
azuredb_cen.gold.dim_feeder
azuredb_cen.gold.dim_household
azuredb_cen.gold.dim_operator
azuredb_cen.gold.dim_substation
azuredb_cen.gold.dim_zone

azuredb_cen.gold.fact_grid_load
Dimension Tables
dim_feeder

Contains feeder-related information used for analyzing energy distribution.

dim_household

Contains household-level information used for consumption analysis.

dim_operator

Contains operator information associated with the energy network.

dim_substation

Contains substation information for grid-level analysis.

dim_zone

Contains geographical/operational zone information.

Fact Table
fact_grid_load

Contains measurable energy/grid metrics used for analytical reporting.

The fact table can be connected with the dimension tables to perform analysis such as:

Energy consumption by zone
Grid load by substation
Household consumption
Feeder performance
Energy trends
Operational performance

Energy Analytics Generated

The pipeline generates important energy-related metrics such as:

Energy Consumption

Measures total energy consumed over a particular time period.

Total Energy Consumption
Average Energy Consumption

Calculates average consumption across households, devices, or zones.

Peak Load

Identifies periods where energy demand is highest.

Grid Load

Measures the amount of load handled by substations and feeders.

Energy Efficiency

Measures how efficiently devices or systems consume energy.

Device Power Usage

Analyzes power consumption across different devices.

Zone-Level Consumption

Compares energy consumption between different zones.

Household Consumption

Identifies high- and low-consumption households.

Feeder Performance

Analyzes load distribution and performance across feeders.

Time-Based Analysis

Energy consumption can be analyzed by:

Hour
Day
Week
Month

This helps identify consumption patterns and peak periods.

Data Quality Testing

The project implements Pytest-based testing for the Silver and Gold layers.

Silver Layer Tests

Examples include:

Table existence validation
Table not-empty validation
Schema/column validation
Null validation
Negative value validation
Runtime validation
Efficiency range validation
Duplicate record validation

Gold Layer Tests

Gold-layer tests validate:

Dimension table existence
Fact table existence
Dimension table not empty
Fact table not empty
Required columns
Null values in important columns
Duplicate records
Valid metric values
Referential integrity
Valid relationships between fact and dimension tables

Monitoring and Alerts

Pipeline monitoring includes:

Databricks job monitoring
Spark logs
Pipeline execution status
Data quality test results
Failed-record tracking
Airflow task monitoring
Delta table monitoring
