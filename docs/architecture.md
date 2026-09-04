# GolStats — Architecture

## Overview

GolStats follows a **Medallion Architecture** implemented in Databricks.

The architecture separates data ingestion, raw storage, transformation, analytical modeling, and downstream consumption into clearly defined stages.

```text
                         StatsBomb Open Data
                                  │
                                  ▼
                           00 · Ingestion
                                  │
                                  ▼
                         Raw JSON · Volume
                                  │
                                  ▼
                            01 · Bronze
                                  │
                                  ▼
                            02 · Silver
                                  │
                                  ▼
                             03 · Gold
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
                 Matches        Teams        Players
                    │             │             │
                    └─────────────┼─────────────┘
                                  ▼
                         Data Quality Gate
                                  │
                                  ▼
                         04 · Analysis
                           /          \
                          ▼            ▼
                       Power BI       ML
```

---

## Data Flow

### 00 · Ingestion

The ingestion layer retrieves football event data from the public StatsBomb Open Data repository.

The process:

1. Retrieves the competition match metadata.
2. Identifies the required matches.
3. Downloads the corresponding event JSON files.
4. Stores the raw files in a Unity Catalog Volume.

Raw storage:

```text
/Volumes/golstats/bronze/raw_files
```

The raw files preserve the original StatsBomb JSON structure and are not transformed during ingestion.

---

## 01 · Bronze

The Bronze layer reads the raw JSON event files using Spark.

```text
Raw JSON
   │
   ▼
spark.read.json()
   │
   ▼
Bronze Delta Table
```

The original nested event structure is preserved.

Basic lineage metadata is added to each event:

* Source file path.
* Match ID extracted from the source filename.

Table:

```text
golstats.bronze.eventos_statsbomb
```

Grain:

```text
One row = one StatsBomb event
```

The complete Bronze dataset currently contains:

```text
234,637 events
64 matches
234,637 unique event IDs
```

---

## 02 · Silver

The Silver layer transforms the nested Bronze event structure into a standardized event-level analytical model.

The transformation includes:

* Flattening nested fields.
* Standardizing column names.
* Extracting event identifiers.
* Extracting timestamps.
* Extracting team information.
* Extracting player information.
* Extracting possession information.
* Extracting spatial coordinates.
* Extracting pass attributes.
* Extracting shot attributes.
* Normalizing pass outcomes.

Table:

```text
golstats.silver.eventos
```

Grain:

```text
One row = one StatsBomb event
```

The Silver layer preserves valid event-level records even when an event does not have a player attribution.

This is important because some StatsBomb events represent match-level or team-level actions rather than individual player actions.

---

## 03 · Gold

The Gold layer transforms event-level data into analytical datasets.

The current Gold model contains four datasets.

### Match Gold

```text
golstats.gold.partidos
```

Grain:

```text
One row = one match
```

Current size:

```text
64 matches
```

Contains:

* Match ID.
* Match date.
* Home team.
* Away team.
* Home score.
* Away score.
* Match result.

---

### Team Match Gold

```text
golstats.gold.estadisticas_equipo
```

Grain:

```text
One row = one team in one match
```

Current size:

```text
128 team-match records
```

Metrics include:

* Total events.
* Passes.
* Completed passes.
* Pass completion percentage.
* Shots.
* Goals.
* xG.
* Pressures.
* Ball recoveries.
* Dispossessions.

---

### Player Match Gold

```text
golstats.gold.estadisticas_jugador
```

Grain:

```text
One row = one player in one match
```

Current size:

```text
1,996 player-match records
```

The player aggregation uses `player_id` as the canonical player identifier.

Position is intentionally excluded from the aggregation key because a player can change position during a match.

---

### Player Tournament Gold

```text
golstats.gold.player_tournament
```

Grain:

```text
One row = one player in the tournament
```

Current size:

```text
680 players
```

This dataset aggregates Player Match Gold into tournament-level performance metrics.

Tournament-level pass completion is calculated from aggregated completed passes and total passes rather than averaging match-level percentages.

This prevents matches with different numbers of passes from receiving equal statistical weight.

---

# Data Quality Gate

Data Quality is implemented as a validation stage rather than as an independent storage layer.

The objective is to verify that the analytical datasets remain structurally and logically consistent before downstream consumption.

## Structural Validation

The pipeline validates:

* Expected match count.
* Unique match IDs.
* Total event count.
* Unique event IDs.
* Expected number of teams per match.
* Null player identifiers in player-level datasets.
* Duplicate player-match combinations.
* Duplicate tournament player identifiers.

---

## Metric Reconciliation

The Gold datasets are reconciled against the underlying Silver event data.

Examples:

```text
Silver Events
     │
     ├── Passes ──────────────► Team Gold
     ├── Shots ───────────────► Team Gold
     ├── Goals ───────────────► Team Gold
     ├── xG ──────────────────► Team Gold
     ├── Pressures ────────────► Team Gold
     ├── Ball Recoveries ─────► Team Gold
     └── Dispossessions ──────► Team Gold
```

The same approach is used to validate Player Gold.

---

## Goal Definition

Goal handling requires explicit business rules because StatsBomb event data includes penalty shootout events and own-goal events.

### Regular match goals

Regular goals are calculated from:

```text
Shot
+ Shot Outcome = Goal
+ Period 1–4
```

Penalty shootout goals in period 5 are excluded from player and team performance statistics.

### Own goals

`Own Goal For` events are assigned directly to the team represented by the event's `team` field.

These goals contribute to Team Gold because they affect the team's official score.

However, because they have no player attribution, they are not assigned to individual players.

As a result:

```text
Official match goals       = 172
Player-attributed goals    = 169
Own Goal For events        = 3
```

---

## Match Reconciliation

Match Gold contains official match scores obtained from competition metadata.

Team Gold independently calculates goals from event-level data.

The two datasets were reconciled for all 64 matches.

Result:

```text
Matches checked:       64
Score discrepancies:    0
```

This provides a cross-layer validation between official match metadata and event-derived analytical metrics.

---

# Scaling Strategy

The project was initially developed using a controlled dataset of 10 matches.

The smaller dataset was used to validate:

* Data ingestion.
* Bronze transformations.
* Silver transformations.
* Analytical definitions.
* Data quality rules.
* Gold aggregation logic.

After validation, the pipeline was scaled to the complete competition.

```text
10-match development dataset
            │
            ▼
       Validation
            │
            ▼
64-match production-scale dataset
            │
            ▼
       234,637 events
```

The scaling process uses incremental raw ingestion.

Existing event files are detected before downloading data.

Only missing matches are downloaded, avoiding unnecessary network requests and preserving the existing raw dataset.

The same Bronze → Silver → Gold architecture is then applied to the complete dataset.

---

# Unity Catalog

The project uses Unity Catalog to organize the analytical data layers.

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

Raw files are stored separately in:

```text
/Volumes/golstats/bronze/raw_files
```

This separates raw file storage from managed analytical Delta tables.

---

# Downstream Consumption

The Gold layer is designed to serve as the foundation for downstream analytics.

## Power BI

Power BI will consume the Gold datasets to build analytical dashboards covering:

* Match results.
* Team performance.
* Player performance.
* Passing.
* Shooting.
* xG.
* Tournament rankings.

## Machine Learning

Future machine learning work will use the Gold datasets as the starting point for feature engineering.

Potential use cases include:

* Player profile clustering.
* Player similarity.
* Performance segmentation.
* Predictive modeling.

## Automation

A future version of the pipeline will use Databricks Workflows to automate:

```text
Ingestion
   ↓
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Data Quality
   ↓
Analytics
```

---

# Source Data and Attribution

The football event data is provided by **StatsBomb Open Data**:

https://github.com/statsbomb/open-data

The data is made available by StatsBomb under its applicable terms and license.

Raw JSON files are not versioned in the Git repository.

They are reconstructed automatically by the ingestion process and stored in the Databricks Unity Catalog Volume.

---

# Design Principles

The GolStats architecture follows several principles:

### Separation of concerns

Each layer has a clearly defined responsibility.

### Explicit analytical grain

Every Gold dataset has a documented grain to prevent accidental duplication or incorrect aggregation.

### Reproducibility

The ingestion process can reconstruct the raw dataset from the public source.

### Data lineage

Bronze records retain their source file information.

### Validation before persistence

Important transformations are validated before being persisted as analytical tables.

### Cross-layer reconciliation

Independent datasets are compared to identify inconsistencies.

### Scalability

The transformation logic is designed to operate on the complete competition rather than only the initial development subset.
