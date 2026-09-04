# GolStats — Football Data Engineering & Analytics Pipeline

End-to-end football data engineering and analytics pipeline built with **Databricks Free Edition**, using **PySpark, Spark SQL, Delta Lake, and Unity Catalog**.

The project processes **StatsBomb Open Data** for the **2022 FIFA World Cup**, following a Medallion Architecture (Bronze → Silver → Gold).

The pipeline was initially developed and validated using a controlled subset of 10 matches and subsequently scaled to the complete competition dataset of **64 matches and 234,637 events**.

> See the complete architecture and layer descriptions in [`docs/architecture.md`](docs/architecture.md).

---

## Project Overview

GolStats is designed as an end-to-end data engineering project that transforms raw football event data into analytical datasets suitable for business intelligence and machine learning use cases.

The current pipeline follows this flow:

```text
StatsBomb Open Data
        │
        ▼
    Ingestion
        │
        ▼
   Raw JSON Files
        │
        ▼
      Bronze
        │
        ▼
      Silver
        │
        ▼
       Gold
        │
        ▼
 Data Quality Gate
        │
        ▼
  Analytical Layer
        │
   ┌────┴────┐
   ▼         ▼
Power BI     ML
```

---

## Current Status

### ✅ Completed

The following components have been implemented and validated:

* StatsBomb Open Data ingestion.
* Raw JSON storage in a Unity Catalog Volume.
* Bronze layer using Delta Lake.
* Silver layer with flattened and standardized event data.
* Gold layer with match, team, and player analytics.
* Tournament-level player aggregation.
* Incremental scaling from 10 development matches to all 64 World Cup matches.
* Structural validation across Bronze, Silver, and Gold.
* Duplicate and grain validation.
* Cross-layer reconciliation of official match scores against event-derived goals.
* Handling of own goals and penalty shootout events according to their analytical meaning.

### 🚧 Planned

Future development will extend the existing pipeline with:

* Power BI dashboard built on the Gold datasets.
* Advanced football analytics.
* Feature engineering for machine learning.
* Player profile clustering.
* Predictive modeling.
* Automated pipeline execution using Databricks Workflows.

---

## Dataset

The current dataset represents the complete **2022 FIFA World Cup**:

| Metric                  |   Value |
| ----------------------- | ------: |
| Matches                 |      64 |
| Events                  | 234,637 |
| Teams                   |      32 |
| Player-match records    |   1,996 |
| Tournament players      |     680 |
| Official match goals    |     172 |
| Player-attributed goals |     169 |

The difference between official match goals and player-attributed goals is intentional.

Three `Own Goal For` events do not contain a player attribution and are therefore included in team-level goals but not assigned to individual players.

Penalty shootout goals are also excluded from match/player performance statistics because they are not regular match goals.

---

## Medallion Architecture

### Bronze

The Bronze layer preserves the raw event structure received from StatsBomb while adding basic metadata for data lineage.

Table:

```text
golstats.bronze.eventos_statsbomb
```

Grain:

```text
One row = one StatsBomb event
```

### Silver

The Silver layer transforms the nested StatsBomb event structure into a standardized analytical event model.

Key transformations include:

* Flattening nested fields.
* Standardizing column names.
* Extracting team and player information.
* Extracting event coordinates.
* Extracting pass attributes.
* Extracting shot attributes.
* Normalizing pass outcomes.
* Preserving event-level granularity.

Table:

```text
golstats.silver.eventos
```

Grain:

```text
One row = one StatsBomb event
```

### Gold

The Gold layer contains analytical datasets designed for downstream consumption.

| Table                                | Grain                         | Records |
| ------------------------------------ | ----------------------------- | ------: |
| `golstats.gold.partidos`             | One row per match             |      64 |
| `golstats.gold.estadisticas_equipo`  | One row per team-match        |     128 |
| `golstats.gold.estadisticas_jugador` | One row per player-match      |   1,996 |
| `golstats.gold.player_tournament`    | One row per tournament player |     680 |

---

## Data Quality & Validation

Data quality is treated as a validation gate between data engineering and downstream analytics.

The project includes validations for:

### Structural integrity

* Expected number of matches.
* Unique match identifiers.
* Total event counts.
* Unique event identifiers.
* Expected number of teams per match.

### Analytical grain

* One row per match in Match Gold.
* One row per team-match in Team Gold.
* One row per player-match in Player Gold.
* One row per player in Tournament Player Gold.
* Duplicate detection.

### Metric reconciliation

Event-derived metrics are reconciled against aggregated Gold datasets.

Examples include:

* Passes.
* Shots.
* Goals.
* xG.
* Pressures.
* Ball recoveries.
* Dispossessions.

### Match score reconciliation

Official match scores from StatsBomb competition metadata were compared against goals independently calculated from event-level data.

The reconciliation produced:

```text
64 matches checked
0 score discrepancies
```

This provides an independent cross-layer validation between official match metadata and event-derived analytical metrics.

---

## Scaling Strategy

The pipeline was intentionally developed using a controlled dataset of 10 matches.

This allowed the transformation logic and analytical definitions to be validated before increasing the data volume.

The pipeline was then scaled to the complete competition without changing the underlying Medallion Architecture.

```text
Development
10 matches
    │
    ▼
Validation
    │
    ▼
Production-scale dataset
64 matches
234,637 events
```

The scaling process also uses incremental raw ingestion:

* Existing event files are detected.
* Only missing matches are downloaded.
* Raw files follow a consistent naming convention.
* The complete dataset is then rebuilt through Bronze → Silver → Gold.

---

## Technical Stack

| Technology                  | Purpose                                   |
| --------------------------- | ----------------------------------------- |
| **Databricks Free Edition** | Development and execution environment     |
| **PySpark**                 | Distributed data transformations          |
| **Spark SQL**               | Data exploration and validation           |
| **Delta Lake**              | Transactional table storage               |
| **Unity Catalog**           | Data governance and organization          |
| **Python**                  | Ingestion and supporting logic            |
| **StatsBomb Open Data**     | Football event data                       |
| **Git / GitHub**            | Version control and project documentation |

---

## Unity Catalog Structure

The project uses the following catalog structure:

```text
golstats
│
├── bronze
│   └── eventos_statsbomb
│
├── silver
│   └── eventos
│
└── gold
    ├── partidos
    ├── estadisticas_equipo
    ├── estadisticas_jugador
    └── player_tournament
```

Raw JSON files are stored separately in a Unity Catalog Volume:

```text
/Volumes/golstats/bronze/raw_files
```

---

## Repository Structure

```text
GolStats/
│
├── README.md
│
├── notebooks/
│   ├── 00_ingestion_statsbomb.py
│   ├── 01_bronze_events.py
│   ├── 02_silver_events.py
│   ├── 03_gold_analytics.py
│   └── 04_scale_pipeline.py
│
├── docs/
│   └── architecture.md
│
└── .gitignore
```

The notebooks are stored in **Databricks source format** (`.py`) using:

```text
# COMMAND ----------
```

markers.

This allows them to be imported back into a Databricks workspace and executed as notebooks.

---

## Data Source

The football event data comes from **StatsBomb Open Data**:

https://github.com/statsbomb/open-data

The raw event JSON files are **not versioned in this repository**.

They are downloaded automatically by the ingestion notebook and stored in the Databricks Unity Catalog Volume.

This keeps the Git repository focused on:

* Source code.
* Data transformations.
* Analytical logic.
* Validation logic.
* Documentation.

---

## Reproducibility

To reproduce the project:

1. Create a Databricks Free Edition workspace.
2. Create the `golstats` catalog.
3. Create the `bronze`, `silver`, and `gold` schemas.
4. Create a Volume for raw event files.
5. Import the notebooks from the `notebooks/` directory.
6. Execute the notebooks in order:

```text
00 → 01 → 02 → 03 → 04
```

The ingestion notebook downloads the required StatsBomb event data.

The remaining notebooks transform and validate the data through the Medallion Architecture.

---

## Roadmap

```text
[x] StatsBomb ingestion
[x] Bronze layer
[x] Silver layer
[x] Gold layer
[x] Scale to complete World Cup
[x] Data quality validation
[ ] Power BI dashboard
[ ] Advanced football analytics
[ ] Feature engineering
[ ] Machine Learning
[ ] Databricks Workflows automation
```

---

## Project Objective

The main objective of GolStats is to demonstrate an end-to-end **data engineering workflow** using a realistic analytical dataset.

The project focuses on:

* Data ingestion.
* Distributed transformations.
* Medallion Architecture.
* Data modeling.
* Analytical aggregations.
* Data quality.
* Cross-layer reconciliation.
* Scalability.
* Reproducibility.
* Downstream BI and Machine Learning readiness.

The project will continue evolving as new analytical and machine learning components are added.
