# Boston Blue Bikes ETL Pipeline

A data pipeline to extract, transform and load data from the Boston Blue Bike public repository into GCP.

The data in GCP is transformed to provide tables which supports charts in Looker.

## Motivation

The project was a part of Data Talk Club's Data Engineering Zoomcamp. The goal was to learn about key parts of ETL infrastructure and concepts using a topic that is relatable for a Boston resident like myself. 

## Architecture

1. Create GCP resources with Terraform
2. Orchestrate using Prefect locally
3. Extract data from the Boston Blue Bikes [public repository](https://www.bluebikes.com/system-data)
4. Transform in memory using pandas to change data types and drop unnecessary/invalid data
5. Load into Google Cloud Storage
6. Load into Google Big Query using a DBT External Table
7. Transform into producition tables using DBT in Google Big Query
8. Create Looker dashboard of the station and trip data 

## Result

![[https://github.com/acgallagher/Boston-Blue-Bikes/blob/main/images/Screenshot%20from%202023-03-14%2014-31-43.png]]

![[https://github.com/acgallagher/Boston-Blue-Bikes/blob/main/images/Screenshot%20from%202023-03-14%2014-32-33.png]]