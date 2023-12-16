# About
This project ingests news data from **The Guardian Open API**, stores it in a PostgreSQL database, and prepares it for sentiment analysis. Each news article is linked to manually classified metadata like keywords and sections, enabling exploration of sentiment trends across various categories.

# Data Flow:

## Collection Pipeline
### Data Fetch

1. Scripts fetch news data from The Guardian Open API based on configurable parameters (e.g., dates, sections).
2. JSON responses are transformed into pandas data frames

### Data Transformation and Modeling
1. Data is filtered and flattened to fit the project's goals.
2. Relationships are modeled and data frames are partitioned accordingly.

### Data Storage

1. Transformed data lands in a PostgreSQL database with schemas optimized for efficient analysis.

## Enrichment Pipeline

## Technologies
- Python: requests, pandas, pycopg2
- PostgreSQL database


