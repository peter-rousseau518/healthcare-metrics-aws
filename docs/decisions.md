# Architecture Decision Log: Healthcare Metrics on AWS

This project builds the CMS nursing home pipeline entirely on AWS, from a Google Drive folder to a public dashboard. It is a companion to the [Snowflake/dbt version](https://github.com/peter-rousseau518/healthcare-metrics-pipeline), which solves the same problem on a different platform. Each record gives the context, the decision, the alternatives considered, and the consequences.

![Architecture](images/architecture.png)

---

## ADR-001: An independent project with its own ingestion

**Status:** Implemented

**Context:** The Snowflake project ingests from a shared Drive folder owned by someone else, through a script run manually from a laptop. For this project, all 22 source files were placed in a Drive folder owned by the project author.

**Decision:** This project has its own ingestion, from its own folder, into its own bucket. The Snowflake project is unchanged.

**Alternatives considered:** Pointing both projects at the new folder and a shared ingestion.

**Consequences:** Each project stands alone end to end, and either can be changed or torn down without affecting the other.

---

## ADR-002: One lakehouse bucket with a prefix per zone

**Status:** Implemented

**Decision:** A single bucket, `peter-cms-lakehouse-2026`, holds `landing/`, `bronze/`, `silver/`, `gold/`, `scripts/`, and `athena-results/`. It is private, versioned, and encrypted. Athena results expire after 30 days and old object versions after 30 days.

**Alternatives considered:** A separate bucket per zone.

**Consequences:** Simple IAM and one place to look. Zone-level permissions can still be scoped by prefix if needed.

---

## ADR-003: Google service account, with its key in AWS Secrets Manager

**Status:** In progress

**Context:** Ingestion must run unattended on a schedule. Personal Google OAuth in "testing" mode expires about every 7 days.

**Decision:** A Google service account is given read-only (Viewer) access to the Drive folder. Its JSON key is stored in AWS Secrets Manager (`cms/google-drive-service-account`), readable only by the Glue job role.

**Alternatives considered:** Personal OAuth (expires); publishing the OAuth app (requires Google verification for Drive scopes); a key file in the repository (insecure).

**Consequences:** Ingestion needs no human sign-in. The key never appears in code, logs, or the repository, and it can be rotated in Secrets Manager without code changes.

---

## ADR-004: Ingestion as a Glue Python job using the Google Drive API

**Status:** Planned

**Decision:** A Glue Python shell job lists the Drive folder, downloads each CSV, and writes it to `landing/release=YYYY-MM/`. It records each file's Drive checksum and skips unchanged files.

**Alternatives considered:** AWS Lambda (15-minute limit and packaging overhead for a 209 MB file); a laptop script (not unattended).

**Consequences:** Ingestion is an AWS-native step in the same workflow as every other job, with logs in CloudWatch.

---

## ADR-005: Serverless lakehouse: S3, Glue Data Catalog, Athena

**Status:** In progress

**Context:** Under 1 GB of data, refreshed monthly (Care Compare) and quarterly (PBJ).

**Decision:** Store data as Parquet in S3, catalog it in the Glue Data Catalog, and query it with Athena.

**Alternatives considered:** Amazon Redshift Serverless (meaningful hourly cost when active); Amazon EMR (cluster management this volume doesn't need).

**Consequences:** No servers or clusters, and costs scale with use.

---

## ADR-006: Medallion layers as Glue databases

**Status:** Implemented

**Decision:** `cms_bronze`, `cms_silver`, and `cms_gold`, mirroring the Snowflake project's raw, staging, and marts schemas.

**Consequences:** Each layer is documented and permissioned separately, and the two projects compare one-to-one.

---

## ADR-007: One bronze Glue job per table, from a shared script

**Status:** Planned

**Decision:** 21 bronze jobs, one per source table, all running the same PySpark script with a table parameter. Each converts its CSV to Parquet, keeps every column as text, adds lineage columns, and registers the table in the catalog. A config file lists the tables, and CloudFormation creates one job per entry.

**Alternatives considered:** One job for all tables (a single failure stops every table; less granular logs and retries); separate scripts per table (duplicated code).

**Consequences:** Per-table run history, logs, and retries; bronze jobs run in parallel; one script to maintain.

---

## ADR-008: One silver job and one gold job, written in Spark SQL

**Status:** Planned

**Decision:** A single Glue job builds all silver tables, and a single Glue job builds all gold marts, each running SQL files through Spark SQL. Conventions and metric definitions match the Snowflake project's dbt models.

**Alternatives considered:** Athena SQL (Glue Workflows can't run Athena queries directly); per-table silver and gold jobs (more jobs than the dependencies require).

**Consequences:** Transformations stay in reviewable SQL, while everything runs inside one Glue Workflow.

---

## ADR-009: Glue Workflow for scheduling and orchestration

**Status:** Planned

**Decision:** A Glue Workflow starts on a monthly schedule and chains: ingest → 21 bronze jobs in parallel → silver (once all bronze jobs succeed) → gold → data quality. A failure stops downstream steps.

**Alternatives considered:** Step Functions with EventBridge (more general, but a second service to operate); Amazon MWAA, managed Airflow (a continuously running environment costs more than this pipeline needs).

**Consequences:** Scheduling, dependencies, and run history live in one AWS Glue view.

---

## ADR-010: Data quality with Glue Data Quality rules

**Status:** Planned

**Decision:** Rulesets written in Glue's Data Quality Definition Language (DQDL) check key uniqueness, completeness, valid values, and row counts, mirroring the Snowflake project's dbt tests. A failed check fails the workflow.

**Consequences:** Quality checks are AWS-native, and results are recorded with each run.

---

## ADR-011: Dashboard on Streamlit Community Cloud, querying Athena

**Status:** Planned

**Decision:** The Streamlit dashboard is hosted free on Streamlit Community Cloud and queries the gold layer through Athena. It authenticates as a dedicated IAM user that can only run queries in the `cms_lakehouse` workgroup, read `gold/`, and write query results. Its access keys are stored only in Streamlit's secrets.

**Alternatives considered:** AWS App Runner (fully AWS, keyless through an IAM role, but a monthly cost); Amazon QuickSight (per-user licensing).

**Consequences:** Free hosting and a public link. Long-lived access keys are the tradeoff, mitigated by narrow permissions and rotation.

---

## ADR-012: Infrastructure as code with CloudFormation

**Status:** Implemented (foundation stack)

**Decision:** All AWS resources are defined in CloudFormation templates in `infrastructure/cloudformation/`, validated with `cfn-lint`, and deployed from the console.

**Alternatives considered:** Console clicks (not reproducible); Terraform (an additional tool).

**Consequences:** The environment can be rebuilt or torn down from version-controlled files.

---

## ADR-013: Cost guards

**Status:** Implemented

**Decision:** An Athena workgroup (`cms_lakehouse`) enforces its settings and caps each query at 10 GB scanned; query results and old object versions expire after 30 days.

**Consequences:** A runaway query can't produce a large bill, and storage doesn't accumulate.

---

## ADR-014: Least-privilege identities

**Status:** In progress

**Decision:**
- `cms-lakehouse-glue-role`: read/write on the lakehouse bucket and read on the Google key secret, plus standard Glue permissions.
- The Google service account: Viewer on one Drive folder.
- The dashboard IAM user: Athena queries in one workgroup and read access to gold.

**Consequences:** Each identity can do only its own job.

---

## ADR-015: Quality of Care page scope

**Status:** Planned

**Decision:**
- **30-day readmissions** by facility and state: the claims-based rehospitalization measure for short-stay residents (primary) and the SNF VBP risk-standardized readmission rate (secondary).
- **Residents per nurse vs. complaint rate:** residents per licensed nurse on duty (24 ÷ licensed-nurse HPRD) against substantiated complaints per 100 residents.
- **Staffing vs. readmissions:** rank correlation and banded averages between PBJ nurse hours per resident day and readmission rates.
- **Not included:** patient satisfaction (nursing home Care Compare has no satisfaction survey) and readmissions by diagnosis category (requires patient-level claims data that isn't public).

**Consequences:** Every metric on the page is supported by the data. The page notes that readmissions, complaints, and staffing cover different periods.
