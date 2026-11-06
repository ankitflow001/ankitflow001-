# Ankit Sharma

**Building Flowtonne** — a SQL-first data platform with a native C++ vectorized query engine, a C++ streaming runtime, and a multi-tenant control plane. Query data where it lives — HDFS, S3, local — without migration.

Also contributing to **Apache Kafka**, **Apache Flink**, and **Apache Iceberg**.

---

## Flowtonne — the platform I'm building

A Snowflake-style analytics platform, from scratch, around three custom-built layers:

```
React UI ──► Java control plane (auth, RBAC, tenants, catalog)
                    │
        Arrow Flight│        ┌─────────────────────────────┐
                    ▼        │ FlowStream (C++ streaming)  │
        ┌──────────────────┐ │ windows, watermarks, state, │
        │  flowtonne-engine│ │ checkpoints, exactly-once   │
        │  C++20 vectorized│ └──────────────┬──────────────┘
        │  SQL engine      │                │
        └────────┬─────────┘                ▼
                 │              file:// · hdfs:// · s3://
                 │              CSV · Parquet · Iceberg
```

### `flowtonne-engine` — native C++ SQL engine
- C++20 vectorized, columnar execution — 2,048-row batches, arena allocation, validity bitmaps
- Full SQL pipeline: lexer → parser → binder → logical/physical planner → optimizer → Volcano operators
- Joins (hash + broadcast), GROUP BY, window functions, CTEs, `DISTINCT`, `BETWEEN`/`IN`
- **External tables over object storage** — Hive-style partition discovery (`country=NG/date=...`), synthesized partition columns, and **partition pruning** via the real expression evaluator
- Apache Arrow Flight server + distributed coordinator for fan-out scans
- Backends: `file://`, `hdfs://` (WebHDFS), `s3://` — CSV, Parquet, Iceberg
- **165/165 tests green**

### `flowtonne-stream` — C++ streaming runtime
- A Flink-style stream processor written natively in C++ (not an embedding)
- Tumbling / sliding / session windows, event-time watermarks, allowed lateness, side outputs
- Keyed Value/List/Map state, aligned checkpoints, savepoints
- Exactly-once two-phase-commit file sinks; native Kafka wire-protocol consumer (no librdkafka)
- Parquet / S3 / Iceberg sinks; interval and window joins
- **293/293 tests green**

### `flowtonne-platform` — control plane + UI
- Java 21 zero-dependency backend (`com.sun.net.httpserver`) — auth, RBAC, multi-tenancy, catalog, ingestion, managed Kafka on Kubernetes via Strimzi
- React 18 + Vite + Tailwind frontend — SQL workspace, streaming jobs, Kafka Connect with Debezium CDC, HDFS browser, dashboards

---

## Open source contributions

| Project | Contribution | Status |
|---|---|---|
| **Apache Kafka** | [PR #24076](https://github.com/apache/kafka/pull/24076) — add missing `@Override` annotations in `clients` | **2 approvals**, awaiting merge |
| **Apache Kafka** | Follow-up PR — `@Override` coverage across the clients module (59 files, 90 methods), suggested by a Kafka committer | Submitted |
| **Apache Flink** | [FLINK-31737](https://issues.apache.org/jira/browse/FLINK-31737) — hide WebUI rescale controls for batch jobs (AdaptiveBatchScheduler can't rescale) | Submitted |
| **Apache Iceberg C++** | [PR #1081](https://github.com/apache/iceberg-cpp/pull/1081) — fix `-Wunused-parameter` / `-Wshadow` CI warnings | Under review |

---

## Tech I work in

`C++20` `Java 21` `TypeScript` `React` `Apache Arrow` `Apache Parquet` `Apache Iceberg`
`Apache Kafka` `Kubernetes` `Strimzi` `Debezium` `Docker` `CMake` `Vite` `Tailwind`

## Currently working on

- ORC reader + Parquet row-group predicate pushdown (Airtel-scale partitioned CDR workloads)
- `abfs://` / `gs://` storage backends
- FlowStream → Iceberg upsert writes and a generic HTTP/API source connector

---

*Most of Flowtonne lives in a private repo today. Public work is the Apache contributions above.*
