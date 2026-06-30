# Snowflake Account Migration: Business Critical (BCS) → Virtual Private Snowflake (VPS)
### Migration Checklist & Cutover Runbook

**Document Classification:** Internal — Data Engineering / Architecture
**Version:** 1.0

---

## Table of Contents

1. [Scope & Objective](#1-scope--objective)
2. [Key Migration Complexities](#2-key-migration-complexities)
3. [Pre-Migration Checklist](#3-pre-migration-checklist)
4. [Identify Unsupported Objects](#4-identify-unsupported-objects)
5. [Identify Cross-Database Views, Joins & Repo References](#5-identify-cross-database-views-joins--repo-references)
6. [Replication Strategy: Group Sequencing](#6-replication-strategy-group-sequencing)
7. [Stream Rebuild](#7-stream-rebuild)
8. [Repository & Connection String Remediation](#8-repository--connection-string-remediation)
9. [Inbound Data Share → Private Listing Conversion](#9-inbound-data-share--private-listing-conversion)
10. [Permissions & Object Validation](#10-permissions--object-validation)
11. [Cutover Runbook — Execution Sequence](#11-cutover-runbook--execution-sequence)
12. [Disaster Recovery for VPS](#12-disaster-recovery-for-vps)
13. [Known Unknowns & Risk Register](#13-known-unknowns--risk-register)
14. [Sign-Off](#14-sign-off)

---

## 1. Scope & Objective

This runbook governs the migration of all production Snowflake workloads from the existing Business Critical (BCS) account to the new Virtual Private Snowflake (VPS) account. It covers account preparation, unsupported-object discovery, replication-group sequencing, cross-database reference remediation, data-share-to-private-listing conversion, stream rebuilds, region-less URL cutover with DR, and post-migration validation.

**In scope:**
- All production databases, schemas, and replicated objects in BCS
- Cross-database SQL views, stored procedures, and application repositories
- Streams, tasks, and CDC pipelines feeding downstream ETL/Liquibase jobs
- Inbound data shares from external providers (must convert to private listings on VPS)
- Connection strings / JDBC / ODBC / BI tool endpoints across all repos

**Out of scope:**
- Non-production (dev/sandbox) databases unless explicitly flagged by application owners
- Historical Time Travel / Fail-safe data beyond the retention window at cutover

---

## 2. Key Migration Complexities

| Complexity | Impact | Mitigation |
|---|---|---|
| Inbound data shares (provider → BCS) | Shared databases cannot be replicated by Snowflake natively | Provider must issue a new Private Listing direct to the VPS account; re-mount as new shared database |
| Cross-database views / joins | Replication cannot resolve references across databases unless both are in the same replication/failover group | Discover via `OBJECT_DEPENDENCIES` + `QUERY_HISTORY`; group dependent DBs together |
| Streams | Streams cannot be created directly via replication on certain source types; stream offset/position is account-specific | Identify all streams pre-cutover; rebuild manually on VPS post-refresh; validate offset reset impact on consumers |
| Unsupported objects | Pipes, externally shared databases, materialized views referencing cross-DB objects, network policies, etc. are not (fully) replicated | Inventory via SHOW/INFORMATION_SCHEMA queries; capture DDL; recreate manually |
| Hard-coded connection strings | Account-locator-based URLs break when accounts change | Standardize all repos on a region-less (org-account name) URL ahead of cutover |
| DR continuity | VPS replaces BCS as primary; new DR pairing required | Stand up VPS failover/DR account; issue second region-less URL for DR; re-point app config (not hard fail-back to BCS) |

---

## 3. Pre-Migration Checklist

| # | Task | Owner | Status |
|---|---|---|---|
| 1 | Provision VPS account; confirm edition, region, and PrivateLink/PSC connectivity | Cloud/Platform Team | ☐ |
| 2 | Confirm org-level replication is enabled between BCS and VPS accounts (ACCOUNTADMIN) | Snowflake Admin | ☐ |
| 3 | Request Private Listings from all external data-share providers, targeted at VPS account | Data Architect | ☐ |
| 4 | Run unsupported-object discovery queries on BCS (Section 4) | Data Engineering | ☐ |
| 5 | Run cross-database reference discovery using ACCOUNT_USAGE + QUERY_HISTORY (Section 5) | Data Engineering | ☐ |
| 6 | Define replication/failover groups: cross-ref clusters vs. independent databases (Section 6) | Data Architect | ☐ |
| 7 | Inventory all streams and their source objects; capture DDL (Section 7) | Data Engineering | ☐ |
| 8 | Inventory all repos/jobs with hard-coded Snowflake connection strings (Section 8) | App/DevOps Teams | ☐ |
| 9 | Define region-less URL standard and update DNS/CNAME if using PrivateLink | Cloud/Platform Team | ☐ |
| 10 | Validate role/grant model and warehouse sizing requirements on VPS | Data Architect | ☐ |
| 11 | Confirm RPO/RTO and freeze window with business stakeholders | Project Owner | ☐ |

---

## 4. Identify Unsupported Objects

Object types that fail or are silently excluded during database/account replication: PIPES (Snowpipe is never replicated), databases created from inbound SHARES, materialized views with cross-database ID-based references, foreign keys / sequences referencing objects in a different database, masking/row-access policies or tags attached across databases, and APPEND-ONLY streams on replicated source objects.

### 4.1 Discover Pipes (not replicated — must be manually recreated)

```sql
-- Run on BCS source account
SHOW PIPES IN ACCOUNT;

SELECT pipe_catalog, pipe_schema, pipe_name, definition,
       notification_channel, last_ingested_status
FROM   SNOWFLAKE.ACCOUNT_USAGE.PIPES
WHERE  deleted IS NULL;
```

### 4.2 Discover Databases Created From Inbound Shares

```sql
SHOW SHARES;

SELECT database_name, database_owner, origin, kind
FROM   SNOWFLAKE.ACCOUNT_USAGE.DATABASES
WHERE  type = 'IMPORTED DATABASE'
AND    deleted IS NULL;
```

### 4.3 Discover Materialized Views / Cross-DB Foreign Keys / Sequences

```sql
-- Materialized views (validate before relying on replication)
SHOW MATERIALIZED VIEWS IN ACCOUNT;

-- Foreign keys and sequences that may cross database boundaries
SELECT *
FROM   SNOWFLAKE.ACCOUNT_USAGE.TABLE_CONSTRAINTS
WHERE  constraint_type = 'FOREIGN KEY';
```

### 4.4 Get DDL for All Unsupported Objects (run per object)

```sql
SELECT GET_DDL('PIPE',  '<db>.<schema>.<pipe_name>');
SELECT GET_DDL('VIEW',  '<db>.<schema>.<view_name>');
SELECT GET_DDL('TABLE', '<db>.<schema>.<table_name>');
SELECT GET_DDL('DATABASE', '<db_name>');
```

> **Note:** Capture DDL output to a script repository (versioned, e.g. Liquibase changelog) before cutover — this becomes the manual-rebuild source of truth for VPS.

---

## 5. Identify Cross-Database Views, Joins & Repo References

Two complementary methods are used: static dependency analysis (OBJECT_DEPENDENCIES) and dynamic usage analysis (QUERY_HISTORY) to catch ad-hoc cross-database joins not captured by view DDL alone.

### 5.1 Static: Object Dependency Graph

```sql
SELECT referencing_database, referencing_schema, referencing_object_name,
       referencing_object_domain,
       referenced_database,  referenced_schema,  referenced_object_name,
       referenced_object_domain
FROM   SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE  referencing_database <> referenced_database
ORDER  BY referencing_database, referenced_database;
```

### 5.2 Dynamic: Cross-Database Joins From Query History

Parse executed SQL text for multi-database references (qualified identifiers). Adjust the lookback window to cover at least one full business cycle (recommend 90 days).

```sql
SELECT query_id, user_name, warehouse_name, start_time,
       query_text
FROM   SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE  start_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
AND    execution_status = 'SUCCESS'
AND    (
         REGEXP_COUNT(UPPER(query_text), '[A-Z0-9_]+\\.[A-Z0-9_]+\\.[A-Z0-9_]+') > 1
       )
ORDER  BY start_time DESC;
```

Post-extraction, group by distinct database-pair combinations to build the cross-reference matrix that drives replication-group membership:

```sql
-- Example downstream aggregation (after extracting DB names via regex/parsing layer)
SELECT db_pair, COUNT(*) AS query_count, COUNT(DISTINCT user_name) AS distinct_users
FROM   parsed_cross_db_queries
GROUP  BY db_pair
ORDER  BY query_count DESC;
```

### 5.3 Repository (Code) Scan for Connection & Cross-DB SQL References

Run alongside the SQL-side discovery to catch hard-coded references inside ETL/Liquibase/app repos:

```bash
# Find hard-coded Snowflake account URLs / locators in repos
grep -RniE "snowflakecomputing\.com|account_locator|jdbc:snowflake" /path/to/repos

# Find cross-database qualified references in SQL/Liquibase changelogs
grep -RniE "[A-Z0-9_]+\.[A-Z0-9_]+\.[A-Z0-9_]+" /path/to/repos --include=*.sql
```

> **Note:** Output of Sections 5.1–5.3 feeds directly into Section 6 — any database pair appearing in dependencies, query history, or repo code must be placed in the same replication/failover group.

---

## 6. Replication Strategy: Group Sequencing

Databases with no cross-references can replicate independently and in parallel. Databases sharing views, foreign keys, sequences, or tags must be replicated together in a single group — replication cannot resolve ID-based references across group boundaries.

### 6.1 Group A — Independent Databases (no cross-reference)

```sql
CREATE FAILOVER GROUP fg_independent_01
  OBJECT_TYPES   = DATABASES
  ALLOWED_DATABASES = DB_SALES, DB_FINANCE_STAGING
  ALLOWED_ACCOUNTS  = <org_name>.<vps_account_name>
  REPLICATION_SCHEDULE = '60 MINUTE';
```

### 6.2 Group B — Cross-Referenced Cluster (joined views, FKs, shared sequences)

```sql
-- All databases participating in cross-DB views/joins MUST be in one group
CREATE FAILOVER GROUP fg_crossref_cluster_01
  OBJECT_TYPES   = DATABASES, ROLES
  ALLOWED_DATABASES = DB_CORE, DB_REFERENCE, DB_REPORTING
  ALLOWED_ACCOUNTS  = <org_name>.<vps_account_name>
  REPLICATION_SCHEDULE = '60 MINUTE';
```

### 6.3 Create Secondary Group on VPS (target account)

```sql
-- Run on VPS target account
CREATE FAILOVER GROUP fg_crossref_cluster_01
  AS REPLICA OF <org_name>.<bcs_account_name>.fg_crossref_cluster_01;

ALTER FAILOVER GROUP fg_crossref_cluster_01 REFRESH;
```

### 6.4 Monitor Replication Status

```sql
SELECT *
FROM   SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
ORDER  BY start_time DESC;

SELECT *
FROM   SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUPS;
```

> **Note:** Databases created from inbound shares CANNOT be added to a replication/failover group. They must be re-established on VPS via a fresh Private Listing from the provider (see Section 9).

---

## 7. Stream Rebuild

Streams on replicated source objects are subject to restrictions: append-only streams are not supported on replicated sources, and a stream's source object must be in the same replication group as the stream itself. Stream offsets do not carry meaningful continuity guarantees across a cross-account migration — plan for a controlled offset reset with downstream consumers.

### 7.1 Inventory Existing Streams

```sql
SHOW STREAMS IN ACCOUNT;

SELECT stream_catalog, stream_schema, stream_name, table_name,
       stream_type, stale, mode
FROM   SNOWFLAKE.ACCOUNT_USAGE.STREAMS
WHERE  deleted IS NULL;
```

### 7.2 Capture DDL Per Stream

```sql
SELECT GET_DDL('STREAM', '<db>.<schema>.<stream_name>');
```

### 7.3 Recreate Stream on VPS (post database refresh)

```sql
USE DATABASE DB_CORE;
USE SCHEMA   PUBLIC;

CREATE OR REPLACE STREAM stream_orders_cdc
  ON TABLE DB_CORE.PUBLIC.ORDERS
  APPEND_ONLY = FALSE
  SHOW_INITIAL_ROWS = FALSE;
```

> **Note:** Coordinate the stream rebuild cutover with downstream task/ETL owners — a new stream starts capturing changes from its creation point; any unconsumed BCS stream offset must be drained or reconciled before cutover.

---

## 8. Repository & Connection String Remediation

Standardize every application, ETL, Liquibase, and BI connection on the region-less (organization + account name) URL format before cutover. This removes dependency on the legacy account-locator/region URL, which changes between accounts and breaks on any future account move.

### 8.1 Identify the Region-less URL

```sql
-- Run on each account to confirm org and account name
SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();

-- Resulting region-less URL format:
-- https://<org_name>-<account_name>.snowflakecomputing.com

-- If using PrivateLink/Private Service Connect, retrieve the private URL set:
SELECT SYSTEM$GET_PRIVATELINK_CONFIG();
```

### 8.2 Repo Scan & Replace (Phase 1 — point at BCS using region-less URL)

```bash
# Identify all files referencing the legacy locator-based URL
grep -Rl "<account_locator>.<region>.snowflakecomputing.com" /path/to/repos

# Replace with region-less URL (still pointing at BCS)
grep -Rl "<account_locator>.<region>.snowflakecomputing.com" /path/to/repos \
  | xargs sed -i 's#<account_locator>\.<region>\.snowflakecomputing\.com#<org_name>-<bcs_account_name>.snowflakecomputing.com#g'
```

### 8.3 Repo Scan & Replace (Phase 2 — cutover to VPS region-less URL)

```bash
grep -Rl "<org_name>-<bcs_account_name>.snowflakecomputing.com" /path/to/repos \
  | xargs sed -i 's#<org_name>-<bcs_account_name>\.snowflakecomputing\.com#<org_name>-<vps_account_name>.snowflakecomputing.com#g'
```

Apply the same pattern to: JDBC/ODBC DSNs, Airflow/orchestration connections, Liquibase changelog connection properties, BI tool (Tableau/PowerBI) data sources, and CI/CD pipeline secrets/variables.

---

## 9. Inbound Data Share → Private Listing Conversion

Databases created from a provider's direct share cannot be replicated by Snowflake. The data-share provider must publish a Private Listing targeted at the VPS account; VPS then creates a new database from that listing. Treat this as a coordinated, provider-side action item with its own lead time.

| Step | Action | Owner |
|---|---|---|
| 1 | Identify all inbound shares in BCS (Section 4.2) and their providers | Data Architect |
| 2 | Request provider to create/publish a Private Listing to VPS account (org.account) | Data Architect / Provider |
| 3 | Provider grants VPS account access to the listing via Provider Studio | Provider |
| 4 | On VPS, accept and create database from the listing | Snowflake Admin |
| 5 | Validate row counts / schema parity vs. the BCS shared database | Data Engineering |

```sql
-- On VPS, after the listing is shared:
SHOW AVAILABLE LISTINGS;

CREATE DATABASE DB_VENDOR_FEED FROM LISTING '<listing_global_name>';
```

---

## 10. Permissions & Object Validation

Privilege grants on database objects are NOT replicated by database-level replication; only account-level replication of ROLES (with the grants applied to those roles) carries privileges across. Validate explicitly post-refresh.

```sql
-- On BCS (source) — export grants baseline
SHOW GRANTS ON DATABASE DB_CORE;
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE  deleted_on IS NULL
AND    table_catalog = 'DB_CORE';

-- On VPS (target) — compare post-refresh
SHOW GRANTS ON DATABASE DB_CORE;
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE  deleted_on IS NULL
AND    table_catalog = 'DB_CORE';
```

Reconcile any gaps with explicit `GRANT` statements scripted from the BCS baseline, applied to the equivalent roles on VPS.

---

## 11. Cutover Runbook — Execution Sequence

Execute in the order below. Steps 1–6 are pre-cutover (no production impact); steps 7–12 are cutover-window activities.

| Phase | Step | Action | Validation |
|---|---|---|---|
| Pre-Cutover | 1 | Prepare VPS account: edition, network policy, PrivateLink/PSC, RBAC bootstrap | Account provisioned; ACCOUNTADMIN access confirmed |
| Pre-Cutover | 2 | Run unsupported-object discovery (Sec. 4) and capture all DDL | DDL inventory stored in version control |
| Pre-Cutover | 3 | Run cross-reference discovery via OBJECT_DEPENDENCIES, QUERY_HISTORY, repo scan (Sec. 5) | Cross-reference matrix signed off by app owners |
| Pre-Cutover | 4 | Create and start replication/failover groups — independent DBs and cross-ref clusters (Sec. 6) | REPLICATION_GROUP_REFRESH_HISTORY shows successful refresh |
| Pre-Cutover | 5 | Request and validate Private Listings from data-share providers; create DBs on VPS (Sec. 9) | Row/schema parity confirmed |
| Pre-Cutover | 6 | Repo Phase 1: re-point all code from locator URL to BCS region-less URL (Sec. 8.2) | All repos build/connect successfully via region-less URL to BCS |
| Cutover Window | 7 | Stand up VPS DR/secondary account; issue a second region-less URL for DR (Sec. 12) | DR failover group created and refreshing |
| Cutover Window | 8 | Repo Phase 2: re-point code from BCS region-less URL to VPS region-less URL (Sec. 8.3) | Connectivity test passes against VPS |
| Cutover Window | 9 | Validate all database object permissions BCS → VPS (Sec. 10) | Grant diff = zero unresolved gaps |
| Cutover Window | 10 | Recreate streams and remaining unsupported objects from captured DDL (Sec. 7, 4.4) | Streams active, not stale; pipes/MVs recreated |
| Cutover Window | 11 | Resume ETL, Liquibase, and on-prem replication jobs against VPS | Job run success; row-count reconciliation passes |
| Post-Cutover | 12 | Post-validation on VPS objects; freeze writes on BCS, then decommission/cut off BCS | Sign-off from data owners; BCS marked read-only/retired |

---

## 12. Disaster Recovery for VPS

VPS becomes the new primary; a DR account must be established before BCS is decommissioned so the org is never single-point-of-failure during or after cutover.

- Provision a DR Snowflake account in a different region/cloud platform per business continuity policy
- Create a secondary failover group on the DR account as a replica of the VPS primary failover group
- Issue a distinct region-less URL for the DR account and document it in the connection-failover runbook (not the same URL as primary — client-side failover/redirect is handled separately, e.g. via Client Redirect / connection objects)
- Re-point monitoring and alerting to track DR replication lag (REPLICATION_GROUP_REFRESH_HISTORY)
- Document and test the failover procedure (RTO/RPO) before BCS is fully retired

```sql
-- On DR account
CREATE FAILOVER GROUP fg_crossref_cluster_01
  AS REPLICA OF <org_name>.<vps_account_name>.fg_crossref_cluster_01;

ALTER FAILOVER GROUP fg_crossref_cluster_01 REFRESH;
```

---

## 13. Known Unknowns & Risk Register

Items below are commonly under-scoped in Snowflake-to-Snowflake migrations and should be explicitly tracked, owned, and closed before final cutover.

| Risk / Unknown | Why It Matters | Recommended Action |
|---|---|---|
| Snowpipe (auto-ingest) configuration | Pipes are never replicated; cloud notification integrations (SQS/Event Grid/Pub-Sub) are account-specific and must be re-wired to VPS storage integration | Recreate pipes manually; re-create cloud notification channel pointing at VPS |
| External tables / external volumes | Storage integration and stage URLs are account- and cloud-credential-specific | Re-create storage integrations and external tables on VPS with new credentials |
| Tasks and task graphs (DAGs) | Tasks tied to streams may reference offsets that do not transfer cleanly; task ownership roles must exist on VPS | Re-deploy task DDL after stream rebuild; validate root task scheduling |
| Time Travel / Fail-safe history | Replication does not carry historical Time Travel data beyond the source's current retention window | Confirm no downstream process depends on querying pre-cutover Time Travel on VPS |
| Query/result cache warm-up | Newly refreshed VPS warehouses start cold; first-run query SLAs may regress temporarily | Pre-warm key warehouses with representative queries before cutover, if SLA-sensitive |
| Network policies, OAuth/SAML, SCIM integrations | These are account-level objects requiring explicit OBJECT_TYPES inclusion in the failover group, or manual reconfiguration | Confirm integration replication scope; re-test SSO end-to-end on VPS pre-cutover |
| Session/masking policies referencing tags across databases | Dangling references across replication-group boundaries fail the refresh | Co-locate policy and tag databases in the same group; test refresh before scaling out |
| Sequences shared across databases | ID-based references fail snapshot replication if the referencing DB and sequence DB are split across groups | Audit via TABLE_CONSTRAINTS / sequence usage; consolidate into one group |
| Stream consumer lag at cutover | In-flight stream consumers on BCS may have unconsumed change data at the cutover instant | Define a hard freeze window; drain BCS streams before final cutover |
| Third-party tool re-authentication | BI/ETL tools cached credentials/OAuth tokens against the old account identifier | Inventory and re-authenticate all third-party integrations post region-less URL switch |

---

## 14. Sign-Off

| Role | Name | Date | Approval |
|---|---|---|---|
| Data Architect | | | ☐ |
| Snowflake Account Admin | | | ☐ |
| Application/ETL Owner | | | ☐ |
| Security / Compliance | | | ☐ |
| Business Stakeholder | | | ☐ |
