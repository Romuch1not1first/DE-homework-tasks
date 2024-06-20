# Homework Summary

## Lesson 2

1. **Created a working branch in the GitHub repository.**
2. **Developed the first job:**
   - Fetches sales data from an API and saves it to the raw directory.
   - Implemented a Flask web server to handle POST requests.
3. **Developed the second job:**
   - Converts JSON files from raw to Avro and saves them to the stg directory.
4. **Verified the job functionality to ensure correctness.**

## Lesson 3

### Homework Summary

1. **Installed Docker Compose on the local computer.**
2. **Set up a demo database:**
   - Cloned the Pagila demo database repository.
   - Started the demo database using Docker Compose and populated it with data.
   - Connected to the demo database via DBeaver using credentials from docker-compose.yml.
3. **Copied the assignment template file to the repository in the `lesson_03` folder:**
   - Created a working branch.
   - Wrote SQL code for all tasks in the template file.

### Notes

1. SQL code is written in a consistent style (all expressions in uppercase).
2. Documented CTE and SubQuery for easier understanding.
3. Added a `.env` file for the docker-compose service.

## Lesson 7

### Homework Summary

1. **Installed Apache Airflow:**
   - Created a new virtual environment for Airflow.
   - Installed Airflow using the provided link.
   - Set the `AIRFLOW_HOME` environment variable to the absolute path of the `airflow` directory.
2. **Created a `dags` directory:**
   - Set the absolute path to the `dags` directory in the `airflow/airflow.cfg` file.
3. **Created the `process_sales.py` file in the `dags` directory:**
   - Created a DAG with `dag_id="process_sales"` consisting of two tasks:
     - `task_id="extract_data_from_api"` — triggers the first job from Lesson 2.
     - `task_id="convert_to_avro"` — triggers the second job from Lesson 2.
   - Tasks complete successfully upon receiving a 201 response from the server.
4. **Configured the DAG:**
   - Processes data for the period from 2022-08-09 to 2022-08-11.
   - Starts daily at 1:00 AM UTC.
   - Considers `raw_dir` and `stg_dir` parameters for each date.
   - Parameters `max_active_runs=1` and `catchup=True` are set.
5. **Workflow Testing:**
   - Ran the first job in one terminal and the second job in another.
   - Started Airflow in a separate terminal.
   - Verified the creation of folders with dates and data in the `storage` directory.

## Lesson 10

### Homework Summary

1. **Logged into Google Cloud Platform (GCP).**
2. **Created a project in GCP:**
   - Named the project DE-07.
   - Project ID: de-07-firstname-lastname.
3. **Created a billing account and linked it to the project.**
4. **Installed the gcloud console utility globally.**
5. **Logged into the gcloud utility.**
6. **Created a bucket in Cloud Storage:**
   - Unique bucket name.
7. **Created a pipeline in Apache Airflow:**
   - Locally installed and configured Airflow.
   - The pipeline uploads data from the API for two dates.
   - Each run uploads data for only one date, using `execution_date`.
8. **Pipeline Testing:**
   - Successfully uploaded files for 2 dates.
   - Files uploaded to the bucket at the path: `src1/sales/v1/2022/08/01/`.

## Lesson 14

### Homework Summary

1. **Configured PySpark on the local computer:**
   - Installed JVM by downloading the appropriate file from the official site.
   - Created a new Python virtual environment and installed dependencies from `requirements.txt`.
   - Verified the PySpark installation:
     - Started the Python interpreter.
     - Executed commands to check the PySpark version:
       ```python
       from pyspark.sql import SparkSession
       spark = SparkSession.builder.master('local[3]').getOrCreate()
       print(spark.version)
       spark.stop()
       ```
     - Ensured PySpark works correctly.

2. **Copied the assignment directory to the repository.**

3. **Started Jupyter Notebook and filled in the assignment template:**
   - Completed tasks using the DataFrame API instead of SQL.

4. **Ensured that the results of each task are displayed in the `.ipynb` file using the `.show()` command.**

## Final Project

Link: [Final Project README](https://github.com/Romuch1not1first/DE-homework-tasks/blob/main/final_project/README.md)