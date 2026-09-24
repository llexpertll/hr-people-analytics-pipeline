# HR Data Pipeline & People Analytics

> 🎓 Independent Study (IS) · M.Sc. Artificial Intelligence for Business Analytics, KMITL
> 📝 Full write-up (Thai): **[พัฒนา Data Pipeline และ People Analytics สำหรับ HR](https://medium.com/@surasak.chantarach/%E0%B8%9E%E0%B8%B1%E0%B8%92%E0%B8%99%E0%B8%B2-data-pipeline-%E0%B9%81%E0%B8%A5%E0%B8%B0-people-analytics-%E0%B8%AA%E0%B8%B3%E0%B8%AB%E0%B8%A3%E0%B8%B1%E0%B8%9A-hr-0efad6c503f8)**

![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Google Cloud Storage](https://img.shields.io/badge/Cloud_Storage-4285F4?style=flat-square&logo=googlecloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=flat-square&logo=googlebigquery&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-150458?style=flat-square&logo=pandas&logoColor=white)
![Odoo](https://img.shields.io/badge/Odoo_HR-714B67?style=flat-square&logo=odoo&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)

An end-to-end HR analytics platform. Employee data moves from an HRIS (**Odoo HR on PostgreSQL**) into a **Google Cloud Storage** data lake and a **BigQuery** data warehouse. **Apache Airflow (Cloud Composer)** orchestrates each step. The warehouse feeds an **attrition-prediction model** and **Power BI** People Analytics dashboards.

---

## 🏗️ Architecture

```mermaid
flowchart LR
    A[(Odoo HR<br/>PostgreSQL)] -->|Extract| B[(GCS Data Lake<br/>data-lake-hr)]
    B -->|Transform<br/>pandas| B
    B -->|Load| C[(BigQuery<br/>dataset: hr)]
    C --> D[ML: Attrition<br/>Prediction · Python]
    C --> E[Power BI<br/>People Analytics]
    F{{Apache Airflow<br/>Cloud Composer}} -.orchestrates.-> A & B & C
```

---

## 🔄 Airflow DAG — `load_postgres_to_gcs_bq`

The DAG is in [`dags/load_postgres_to_gcs_bq.py`](dags/load_postgres_to_gcs_bq.py). It runs **4 ETL pipelines in parallel**:

```mermaid
flowchart LR
    S([start_pipeline])
    S --> E1[postgres_to_gcs_emp_all] & E2[postgres_to_gcs_emp_address] & E3[postgres_to_gcs_emp_education]
    E1 & E2 & E3 --> T1[transform_emp_data] --> L1[gcs_to_bq_employee] --> F([finish_pipeline])
    S --> A1[postgres_to_gcs_emp_attrition] --> A2[transform_emp_atrrtion] --> A3[gcs_to_bq_emp_attrition] --> F
    S --> V1[postgres_to_gcs_emp_leave] --> V2[gcs_to_bq_emp_leave] --> F
    S --> P1[postgres_to_gcs_emp_performance] --> P2[gcs_to_bq_emp_performance] --> F
```

| # | Pipeline | Source table(s) → BigQuery table | Transform |
|---|---|---|---|
| 1 | Employee master | `employee_all` + `employee_address` + `employee_education` → `hr.employee` | Selects the key columns and left-joins the three tables on `x_emp_id` (pandas) |
| 2 | Attrition | `employee_attrition` → `hr.employee_attrition` | Placeholder step |
| 3 | Leave | `employee_leave` → `hr.employee_leave` | — |
| 4 | Performance | `employee_performance_rating` → `hr.employee_performance_rating` | — |

**Key operators:** `PostgresToGCSOperator` · `PythonOperator` · `GCSToBigQueryOperator` (`WRITE_TRUNCATE`, schema autodetect)
**Schedule:** `@hourly` · retries: 5 (every 5 min)

---

## 🤖 Attrition Prediction & Insights

- Reduced 34 features to 20 key predictors
- Compared **Logistic Regression, Decision Tree, Random Forest, SVM, KNN, Neural Network**
- Dashboard highlights: 1,470 employees · 237 attritions (**~16% attrition rate**) · breakdowns by department, demographics and commute distance

---

## ⚙️ How to Run

1. Set up Airflow 2.x (or Cloud Composer) with the Google and Postgres providers:
   ```bash
   pip install -r requirements.txt
   ```
2. Create two Airflow connections:
   - `postgres_default` → the Odoo HR PostgreSQL database
   - `google_cloud_default` → a GCP service account with GCS and BigQuery access
3. Update the constants at the top of the DAG to match your environment: `GCP_PROJECT`, `GCS_BUCKET`, `GBQ_DS`
4. Copy `dags/load_postgres_to_gcs_bq.py` into your Airflow `dags/` folder, then trigger the DAG

---

## 📁 Repository Structure

```
hr-people-analytics-pipeline/
├── dags/
│   └── load_postgres_to_gcs_bq.py   # Airflow ETL DAG
├── requirements.txt
└── README.md
```

---

👤 **Surasak Chantarach** · [LinkedIn](https://linkedin.com/in/surasak-ch/) · [Medium](https://medium.com/@surasak.chantarach) · [GitHub](https://github.com/llexpertll)
