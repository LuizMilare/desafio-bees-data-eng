# BEES Data Engineering Case - Open Brewery DB API Pipeline



This project contains a scalable data pipeline to extract, transform and persist data from the **Open Brewery DB API** into a *Data Lake* using the *Medallion Architecture*

  
  

## Tools and Technologies

*  **Languages:** Python and PySpark

*  **Orchestration:** Apache Airflow

*  **Quality Tests:** Pytest

*  **Containerization:** Docker and Docker Compose

  

---


## Data Architecture

  

The *Data Lake* was structured in three layers, designed to ensure scalability and data quality:

  

### 1. Bronze Layer (Raw Data)

*  **Goal:** Ingestion of data in its raw form.

*  **Process:** Data is extracted from the API with pagination and persisted in `.json` format. This layer should work as the source of truth.

  

### 2. Silver Layer (Curated Data)

*  **Goal:** Cleaning and optimization.

*  **Transformations:** Define Schema. Deduplicate records by id. Treatment of null values for the `country` column. Conversion from `.json` to **Parquet** for columnar storage.

*  **Partitioning:** Data was partitioned by location (`country`) to reduce Spark's I/O on regional queries.
  

### 3. Gold Layer (Aggregated Data)

*  **Goal:** Provide analytics data for business.

*  **Process:** Create aggregated view with **number of breweries by type and location**, ready to be used by BI tools or analytical reports.

  

---

  

## Data quality and Tests

  

To assure Data Quality, a suite of integration tests was implemented on **Pytest** acting directly on the Airflow DAG as a **Quality Gate**

* The tests occur right after all tasks are completed. But can be adapted to execute after each task which woul be more suitable for a production environment.

*  **Tested Cases:** Existence of the bronze layer raw `.json` file; existence of data inside the `.json` file; existence of data within the silver layer; uniqueness check; existence of main expected columns on the silver layer; existence of aggregation column on gold layer.

* If any test case fails, the test task fails and triggers an error, but the data is still loaded to the Data Lake, since the test task is the last to be executed.

  

---

  

## Alerts and Monitoring

  

In a production environment, the *pipeline* could be monitored as follows:

  

1.  **Pipeline Failure Alerts (Airflow):**  `on_failure_callback` setting on Airflow DAG to notify the engineers immediately via Slack, Microsoft Teams or e-mail in case any task fails. In this pipeline, the `on_failure_callback` setting is active, but it doesn't notify any e-mail. To do this, a SMTP server should be setup.

  

2.  **Monitoring and Data Quality:** The current *Quality Gate* should act as a starting point. In a production environment, tools like *Great Expectations* could be used to validate data anomalies.

  

---

  

## Design Choices and Trade-offs

  

*  **PySpark vs Pandas:** The chosen data processing language was PySpark to demonstrate a *Big Data*-ready architecture. Although Pandas could be used for this particular case, PySpark works better at scale.

*  **Local vs Cloud Storage:** Since it is not specified in the instructions, the *Data Lake* storage and processing was kept on local Docker volumes. In a real architecture, the storage could be migrated to cloud based solutions, such as AWS S3 and GCS, and the processing could be done in a databricks cluster.

*  **Tests Within DAG:** The tests occur within the Airflow DAG instead of a CI/CD pipeline, assuring the data quality is assured on runtime.

  

---

  

## How to Run the project

  

**Pre-requisites:** Have [Docker](https://www.docker.com/) and Docker Compose installed, with enough RAM available.

  


1.  **For Windows users**

* Make sure your IDE is using **LF** (Line Feed) instead of **CRLF** line endings. 
* To accomplish this you can set `git config --global core.autocrlf false` before cloning. Or change your settings from CRLF to LF in your IDE's interface.


2.  **Clone the repository:**

```
git clone https://github.com/LuizMilare/desafio-bees-data-eng.git

cd desafio-bees-data-eng
```
3. **Set Environment Variables**

* Rename .env_example file to .env
* Set 
```
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW_SECRET_KEY=any_key_you_like
```

4.  **Start the infrastructure**

```
docker-compose up -d --build
```

5.  **Access Airflow**

* Open browser in http://localhost:8080.

* User: admin / Password: admin

  

6.  **Execute Pipeline**

* Unpause the brewery_medallion_pipeline DAG to start extration, transformation and tests automatically.

* Check if the data was indeed recorded in \data folder

7. **Monitor DAGs Execution metrics on Grafana**

* Open browser in http://localhost:3000

* User: admin / Password: admin. (Skip if prompted to create new password)

* Access Dashboards > Breweries dashboard  

8.  **Stop Application**

```
docker-compose down -v
```
