# Final Project

## General Information

The final project can be completed in one of two ways:
- Using Google Cloud, specifically GCS and BigQuery services
- Using Pyspark locally

Regardless of the option you choose, data workflow orchestration must be done using Apache Airflow.

If you choose the first option (Google Cloud), an example of orchestrating GCS and BigQuery services can be found in the `example_dag_2.py` file in the folder of the 7th lecture.

If you choose the second option, an example of orchestrating Pyspark jobs can be found in the `example_dag_3.py` file.

**Important!!**
The further description of the task is given assuming that you chose the first approach (Google Cloud). If you choose the second approach, you need to adapt the task description yourself. For this, replace "bucket" or "dataset" with "directory".

Of course, you can complete the task using both options. Although it will not affect the score, it will help better understand the difference between the two approaches.

When using the Pyspark option, make sure to do all the work locally! The same task performed in the cloud will not differ much from the one done locally (except for the DevOps part - deployment, rights management, etc.). However, using Pyspark in the cloud can turn out to be too expensive!

## Prerequisites

1. The data files for the project are located in the `data` directory.
2. You need to create a bucket in GCS for storing raw files (hereinafter - raw-bucket) and copy the files into it.
3. In BigQuery, you need to create the following datasets: `bronze`, `silver`, `gold`.
4. In a real project, the files are located in an external source and there is an ingestion stage. To simplify, we assume that the files are already in the raw-bucket.
5. You need to create a project in GCS. Orchestration should be done using Apache Airflow. Any data movement or transformation must be managed with Apache Airflow.
6. The pull request must contain the commit history according to the project legend. If there is only one commit - you will get **0 points**.

## Legend

You work in the Data department of a company that sells consumer electronics. Your department has been tasked with analyzing sales by the geographical location of buyers (by states) and by age. To complete this task, you have managed to agree on integrating two data sources: `customers` and `sales`. To do this, you need to develop two data pipelines:

## 1. `process_sales` pipeline

You decided to insert data from the raw-bucket into `bronze` by reading the file as an external table (schema-on-read), as the file format is `.csv`, and this method is the safest. You decided to impose the schema so that any field in the file is considered STRING. In the `sales` table in the `bronze` dataset, all fields are also STRING.

You decided to keep the column names in `bronze` from the source file to facilitate error tracking, specifically:
```
CustomerId, PurchaseDate, Product, Price
```

When transferring data to `silver`, you noticed that the data has some "dirt" and decided to clean it.

The schema in `silver` already has types that are convenient for data analysis. The columns are named according to the company's rules, specifically:
```
client_id, purchase_date, product_name, price
```

You decided to partition the data because there is a lot of data and analysts need to make selections based on the date.

The data already comes partitioned - each date in a separate folder.

## 2. `process_customers` pipeline

This data source comes in an unusual way: the data provider did not agree to put data for each day in a separate folder, instead, they dump the entire table every day. That is, each subsequent day contains data for all previous days.

You decide not to partition the data as there is not much of it.

You process the data at the `bronze` and `silver` levels.

In `silver`, the table has the following columns:
```
client_id, first_name, last_name, email, registration_date, state
```

In `bronze`, the column names remain original (as in the CSV file).

---------------------------------------------------------------------------
You run both DAGs, and all data is successfully transferred.

Analysts analyze the data in `silver` and notice that clients did not fill in some data, so they are empty. Most importantly, the state names are missing, which makes it impossible to perform geographic location analysis. Age data of buyers is also missing. Additionally, the company plans to implement customer notifications, and the records of names and surnames are not complete (users tend to fill in either the first name or the last name selectively).

To solve the problems described above, you agree to integrate a third data source: `user_profiles`:

## 3. `process_user_profiles` pipeline

This data is provided in JSONLine format. You build a pipeline and transfer it to the `silver` level. The data has perfect quality.

This pipeline is run manually, not on a schedule.

After all the data is successfully processed, you need to enrich the `customers` data using the `user_profiles` data.

## 4. `enrich_user_profiles` pipeline

This is the only pipeline that writes data to the `gold` level. As a result of this pipeline, a `user_profiles_enriched` table should be created in the `gold` dataset. This table contains all the data from the `silver.customers` table, but all names, surnames, and states should be filled in with data from the `silver.user_profiles` table. Additionally, you need to add all the fields that are in `silver.user_profiles` but missing in `silver.customers` (e.g., `phone_number`).

This pipeline should also be run manually (not on a schedule).

Optionally, you can implement it so that 'process_user_profiles', upon successful execution, triggers 'enrich_user_profiles'.

In the end, when all pipelines are built, try to answer the following analytical question:

> In which state were the most TVs bought by customers aged 20 to 30 in the first decade of September?

## Notes and Tips
1. Find out what the `MERGE` expression does in SQL. It may come in handy.
2. In the context of this task, you should try the following data engineering approaches: Data Cleansing, Data Wrangling, and Data Enrichment.