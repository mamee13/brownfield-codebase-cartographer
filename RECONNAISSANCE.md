# RECONNAISSANCE — ol-data-platform
Date: 2026-03-11
Repo: `.cartography_repos/ol-data-platform`

## Scope and Method
Manual skim (no execution) of:
- `README.md` for system description.
- Dagster ingestion code in `dg_projects/`.
- dbt models in `src/ol_dbt/models/` (staging + dimensional).

## High-Level Map (Manual)
- Dagster is the core orchestration framework for data pipelines. Evidence: `README.md:3-5`.
- Ingestion code lives under `dg_projects/` (example: `dg_projects/data_loading/...`).
- Transformations and marts are implemented in dbt SQL under `src/ol_dbt/models/` (staging, dimensional, reporting, etc.).

## Day-One Questions (Manual Answers)

### 1) What is the primary data ingestion path?
**Answer:** Raw data is ingested from S3 into a raw warehouse schema via a dlt pipeline, then surfaced as dbt `source()` tables in staging models.
- Ingestion: `dg_projects/data_loading/data_loading/defs/edxorg_s3_ingest/loads.py` defines a dlt source that reads S3 TSVs and writes to a raw dataset (`ol_warehouse_*_raw`). Evidence: `dg_projects/data_loading/data_loading/defs/edxorg_s3_ingest/loads.py:1-153`.
- Staging: dbt staging models consume raw tables via `source('ol_warehouse_raw_data', ...)`. Evidence: `src/ol_dbt/models/staging/micromasters/stg__micromasters__app__postgres__auth_user.sql:1-19`.

### 2) What are the 3–5 most critical output datasets/endpoints?
**Answer (inferred from dimensional/fact models):**
- `dim_course_content` — foundational course structure used by multiple models. Evidence: `src/ol_dbt/models/dimensional/dim_course_content.sql:1-174`.
- `afact_course_page_engagement` — engagement fact table built from navigation events + course structure. Evidence: `src/ol_dbt/models/dimensional/afact_course_page_engagement.sql:1-61`.
- `afact_discussion_engagement` — discussion engagement fact table. Evidence: `src/ol_dbt/models/dimensional/afact_discussion_engagement.sql:1-67`.
- `dim_problem` — problem dimension derived from course content and tracking logs. Evidence: `src/ol_dbt/models/dimensional/dim_problem.sql:1-119`.

### 3) What is the blast radius if the most critical module fails?
**Answer:** If `dim_course_content` is wrong or missing, multiple downstream dimensional and reporting models that `ref('dim_course_content')` will be impacted, including `afact_course_page_engagement`, `afact_discussion_engagement`, and `dim_problem`.
- `afact_course_page_engagement` depends on `dim_course_content`. Evidence: `src/ol_dbt/models/dimensional/afact_course_page_engagement.sql:1-12`.
- `afact_discussion_engagement` depends on `dim_course_content`. Evidence: `src/ol_dbt/models/dimensional/afact_discussion_engagement.sql:1-67`.
- `dim_problem` depends on `dim_course_content`. Evidence: `src/ol_dbt/models/dimensional/dim_problem.sql:1-18`.

### 4) Where is business logic concentrated vs. distributed?
**Answer:** Business logic appears concentrated in dbt dimensional and fact models (SQL transformations and aggregations), while ingestion logic is centralized in Dagster/dlt ingestion modules.
- Dimensional/fact logic: `afact_course_page_engagement.sql` and `dim_problem.sql` include joins, filters, and derived fields. Evidence: `src/ol_dbt/models/dimensional/afact_course_page_engagement.sql:1-61`, `src/ol_dbt/models/dimensional/dim_problem.sql:1-119`.
- Ingestion logic: dlt pipeline configuration and S3 read patterns in the Dagster data loading module. Evidence: `dg_projects/data_loading/data_loading/defs/edxorg_s3_ingest/loads.py:1-153`.

### 5) What has changed most frequently in the last 90 days?
**Answer:** Not determined in this manual recon (requires `git log` or velocity analysis). This will be filled by Surveyor’s git-velocity pass in the automated run.

## Open Questions / Unknowns
- The full set of authoritative “top 3–5” output marts likely lives under `src/ol_dbt/models/marts/` or `reporting/`, which were not fully reviewed in this manual skim.
- Precise dependency chains across Dagster assets → dbt sources → marts require automated lineage extraction.
