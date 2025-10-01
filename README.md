# Oracle-to-GCP Migration

> Repository containing code, configuration and documentation for an on-premise Oracle → Google Cloud Platform (GCP) migration. Includes batch and CDC stream pipelines using Datastream, Dataproc Serverless and BigQuery, orchestration with Cloud Composer, and best practices for observability and security.

---

## Repository structure (suggested)

```
/README.md
/.gitignore
/infra/environments/            # Environments (eg. dev, test, prod)
/infra/modules/                 # Terraform modules for GCP services
/code/dags/                     # Cloud Composer DAGs to manage migration by table groups
/code/jobs/                     # Dataproc Serverless jobs for data migration
```

---

## GCP Service Used

* **BigQuery**: target data warehouse for analytics and reporting.
* **Datastream**: change data capture (CDC) from Oracle to GCS / BigQuery.
* **Dataproc Serverless**: run Spark / PySpark jobs for large-scale transformations.
* **Cloud Composer**: orchestration (Airflow) for pipelines and dependencies.
* **Cloud Logging**: centralize logs from jobs and services.
* **Cloud Monitoring**: metrics, alerts and dashboards.
* **Secret Manager**: store credentials and secrets (do not version secrets in repository).

---

## Best practices & considerations

* **Security** — use service accounts with least privilege. Never commit secrets.  
* **Idempotency** — design jobs to be re-runnable and to handle duplicates (e.g., deduplicate in BigQuery using `commit_timestamp` or CDC keys).  
* **Partitioning & clustering** — partition by date and cluster by frequent filter columns to optimize cost & performance in BigQuery.  
* **Cost control** — monitor BigQuery queries (slots, ad-hoc), GCS storage and Dataproc usage; apply bucket lifecycle policies.  
* **Observability** — expose custom metrics if needed (job duration, CDC latency, ingestion lag), and configure alerts for failures and delays.  
* **Testing** — implement automated reconciliation tests between Oracle and BigQuery to validate migration accuracy.

---

## Secret management & security

* Use **Secret Manager** for Oracle credentials, service account keys and other secrets.  
* In Airflow (Cloud Composer), reference secrets from Secret Manager in Connections rather than storing credentials in Airflow variables.  
* Rotate secrets periodically and audit access with Cloud Audit Logs.

---

## Observability & runbooks

* Centralize logs in **Cloud Logging**; create sinks to export logs to BigQuery for historical analysis if needed.  
* Configure **Cloud Monitoring** for:
  * Alerts on DAG failures (Cloud Composer)
  * CDC ingestion lag/latency
  * Dataproc jobs with anomalous run times
* Document runbooks for common incidents: ingestion failures, CDC lag, permission errors, and Spark job failures.

---

## CI/CD and deployment

* If using Terraform, keep reusable modules for common resources.  
* Separate environments (dev, qa, prod) using projects or naming prefixes.  
* Pipelines should include steps for:
  * Linting and unit tests for scripts
  * DAG validation (Airflow)
  * Controlled deployment to Composer and Dataproc

---
