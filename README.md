NBA End-to-End ETL Pipeline

Project Overview

This project implements an end-to-end data engineering pipeline that ingests live NBA game data from a public API, stores immutable raw snapshots, transforms the data into structured tables, and loads it into an analytical warehouse.

The goal is to simulate a production-style data engineering workflow using modern tools and best practices.

Architecture

The pipeline follows a layered architecture:

1. Ingestion Layer
   - Pulls NBA game data from the balldontlie API
   - Stores raw JSON snapshots with timestamps
   - Preserves immutable source data

2. Raw Storage
   - Raw API responses stored in:
     data/raw/
   - Timestamped files allow reproducibility and debugging

3. Transformation Layer
   - Flattens nested JSON
   - Standardizes schema
   - Separates into structured tables

4. Warehouse Layer
   - Loads structured data into DuckDB
   - Implements a star schema model:
     - `fact_game_stats`
     - `dim_team`
     - `dim_date`

Tech Stack

- Python
- Requests (API ingestion)
- Pandas (data transformation)
- DuckDB (analytical warehouse)
- Parquet (columnar storage)
- Git (version control)

Future Enhancements

- Add scheduled batch runs
- Implement incremental loading
- Add data quality checks
- Build analytics queries
- Create dashboard layer
- Add Docker containerization
- Implement CI/CD pipeline
