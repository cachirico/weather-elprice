# NO2 Weather & Electricity Price Analysis

This project builds an end-to-end data pipeline in Databricks that fetches, validates, and analyzes the relationship between historical weather observations and spot electricity prices in price area **NO2 (Southern Norway / Stavanger)** for January–June 2025.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** in Unity Catalog, is orchestrated via **Jobs-as-Code (YAML)**, and delivers insights through **Gen AI summaries**, **MLflow price predictions**, and an **interactive Lakeview Dashboard**.

## 1. Architecture & Data Flow

The pipeline runs as a workflow (`pl_weather_price`) in Databricks, where the final AI summary and machine learning model run in parallel once the data quality validation step passes:

```text
                       [02_bronze_ingestion]
                                 │
                      [03_silver_transformations]
                                 │
                          [04_silver_join]
                                 │
                         [05_gold_analysis]
                                 │
                          [06_validation]
                         /               \
        [07_gen_ai_summary]             [08_predictive_model]
```

### Medallion Layers (`workspace` catalog)

* **Bronze (`bronze.weather_api_raw`, `bronze.electricity_price_api_raw`):**
  * **Purpose:** Raw data storage in Delta format.
  * **Implementation:** Iterates day-by-day to fetch hourly data from both APIs—requesting static daily JSON files from Hvakosterstrømmen and 24-hour `PT1H` observation windows (`referencetime`) from MET Frost. Uses retry logic (`request_with_retry`), strips dynamic API metadata before computing a SHA-256 `payload_hash` and `bronze_record_id`, and writes the raw payloads with run metadata using insert-only `MERGE`.
* **Silver (`silver.weather_hourly`, `silver.electricity_price_hourly`, `silver.weather_price_hourly`):**
  * **Purpose:** Cleaned, typed, and joined hourly data.
* **Implementation:** `03_silver_transformations` selects the latest valid Bronze payload per request, unpacks the JSON structures into typed columns, pivots weather elements (selecting the best quality code if duplicates exist), and provides both UTC (`time_start_utc`) and local (`Europe/Oslo`) timestamps. `04_silver_join` performs a `LEFT JOIN` of hourly electricity prices against weather observations on `time_start_utc`, tagging each row's completeness in `weather_match_status` (`matched`, `partial`, or `missing`) without imputation, and merges the result into `silver.weather_price_hourly`.
* **Gold (`gold.price_by_temp_band`, `gold.executive_summary`):**
  * **Purpose:** Aggregated tables and summaries ready for analysis and reporting.
  * **Implementation:** `05_gold_analysis` groups the hourly data into 5°C temperature intervals and calculates key statistics (average, median, standard deviation, min/max, and number of hours). `07_gen_ai_summary` adds an automated 3-bullet executive summary generated with Databricks Foundation Model APIs (`ai_query`).
* **Operations (`ops.data_quality_results`):**
  * **Purpose:** Logs the results of automated data quality checks across every layer for each `run_id`.

## 2. Data Sources

* **Electricity Prices:** [Hvakosterstrømmen.no API](https://www.hvakosterstrommen.no/strompris-api). Delivers hourly day-ahead spot prices (NOK/kWh) for price area **NO2**. Open API requiring no authentication.
* **Weather Observations:** [MET Frost API](https://frost.met.no/) (`observations/v0.jsonld`). Delivers hourly (`PT1H`) measurements for air temperature (`air_temperature`), wind speed (`wind_speed`), and precipitation (`sum(precipitation_amount PT1H)`) from weather station **SN44560 (Sola / Stavanger)**. Uses a Client ID stored in Databricks Secrets.

## 3. Data Quality, Reliability & Orchestration

* **Idempotency (`MERGE INTO` & Hashing):** 
  * In Bronze, dynamic API metadata is removed before hashing the data with SHA-256 (`canonical_payload_for_hash`), and new rows are added using insert-only `MERGE` on `bronze_record_id`.
  * In Silver and Gold, tables are updated using Delta Lake `MERGE INTO` (upsert combined with a scoped delete for the selected `date_from`–`date_to` period). This makes it safe to re-run the pipeline for the same dates without creating duplicate rows.
* **Error Handling:** API calls handle rate limits (`HTTP 429` with `Retry-After`) and temporary server errors (`HTTP 5xx`) by waiting and retrying automatically.
* **Data Quality Checks (`ops.data_quality_results` and `06_validation`):** Each step checks the data (such as verifying that API requests succeeded, keys are unique, required columns have no nulls, and row counts match between Silver and Gold) and logs the results to `ops.data_quality_results`. If a critical check fails (`ERROR`), the pipeline stops immediately so bad data does not move forward.
* **Workflows & Jobs-as-Code:** The entire Databricks job (`pl_weather_price`), including task dependencies, parameters (`date_from`, `date_to`, `run_id`), table triggers, and Git setup, is saved as YAML in `jobs/pl_weather_price.yml`.

## 4. Analytics, Gen AI & Machine Learning

* **Gen AI Summary (`07_gen_ai_summary`):**
  * Uses Databricks Serverless Foundation Model APIs (`ai_query`) to create a 3-bullet executive summary saved to `gold.executive_summary`.
  * **Avoiding AI Hallucinations:** Because language models can make mistakes when sorting or comparing numbers, Python calculates the exact rankings first (highest/lowest price bands, most common temperature band, and average vs. median differences) and passes them directly into the prompt as fixed facts.
* **Price Prediction Model (`08_predictive_model`):**
  * Trains a `RandomForestRegressor` (`scikit-learn`) on `silver.weather_price_hourly` to predict NO2 electricity prices based on temperature, wind speed, hour of the day, and month.
  * Uses **MLflow** to log model parameters (`n_estimators`, `max_depth`), error metrics (`MAE`, `RMSE`), feature importances, and the trained model itself.
* **Interactive Lakeview Dashboard (`Executive Overview`):**
  * Displays the Gen AI summary at the top, followed by charts showing average vs. median price per temperature band and the number of observed hours, with interactive date and area filters.

## 5. Repository Structure

```text
├── Dashboards/
│   └── NO2 Electricity and Weather Insights.lvdash.json  # Lakeview BI dashboard definition
├── Jobs/
│   └── pl_weather_price.yml                              # Databricks Workflows YAML configuration
├── Notebooks/
│   ├── 01_setup.ipynb               # Creates schemas (bronze, silver, gold, ops) and Delta tables
│   ├── 02_bronze_ingestion.ipynb    # Fetches raw API data into Bronze Delta tables
│   ├── 03_silver_transformations.ipynb # Cleans, types, and formats hourly weather and price data
│   ├── 04_silver_join.ipynb         # Joins hourly weather and electricity prices in Silver
│   ├── 05_gold_analysis.ipynb       # Calculates price statistics by 5°C temperature bands in Gold
│   ├── 06_validation.ipynb          # Runs cross-layer data quality checks
│   ├── 07_gen_ai_summary.ipynb      # Generates the AI executive summary using ai_query()
│   └── 08_predictive_model.ipynb    # Trains the Random Forest model and logs results to MLflow
└── README.md
```

## 6. How to Run

1. **Clone the repository in Databricks:**
   * Go to **Workspace** → **Users** → `<your-user>` → **Create** → **Git folder**.
   * Paste the repository URL: `https://github.com/cachirico/weather-elprice.git` (branch: `main`).
2. **Add your Frost API key to Databricks Secrets (first-time setup):**
   * Register for a free Client ID at [frost.met.no](https://frost.met.no/auth/requestCredentials.html) and run this one-time code in a notebook cell to save the key in the `weather_price` scope:
     ```python
     from databricks.sdk import WorkspaceClient
     w = WorkspaceClient()
     w.secrets.create_scope(scope="weather_price")
     w.secrets.put_secret(scope="weather_price", key="frost_client_id", string_value="<YOUR_FROST_CLIENT_ID>")
     ```
3. **Create schemas and tables:**
   * Run `Notebooks/01_setup` once to set up the `bronze`, `silver`, `gold`, and `ops` schemas and tables in Unity Catalog.
4. **Run the pipeline:**
   * Open the `pl_weather_price` job under **Jobs & Pipelines** (or import `Jobs/pl_weather_price.yml` via *Edit YAML*) and click **Run now**.
   * The pipeline uses **Jobs-as-Code** with `source: GIT`, meaning Databricks automatically pulls and executes the version-controlled notebooks directly from the repository's `main` branch with zero hardcoded workspace paths.