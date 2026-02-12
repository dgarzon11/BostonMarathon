# BostonMarathon

This project analyzes Boston Marathon results data downloaded from the Boston Athletic Association results portal. The dataset for 2025 is sourced from the official download page: [BAA Results Download](https://registration.baa.org/2025/cf/Media/iframe_ResultsSearch.cfm?mode=download&display=yes).

## Goal
Explore and analyze Boston Marathon results to support reporting, exploration, and future analysis.

## Data source configuration
The app uses PostgreSQL (for example Supabase).

### Environment variables

```bash
# postgres / supabase
SUPABASE_DB_URL=postgresql://<user>:<password>@<host>:5432/<database>?sslmode=require
RESULTS_TABLE=marathon_results_2025
# Optional: custom SQL (if set, it takes precedence over RESULTS_TABLE)
# RESULTS_QUERY=SELECT * FROM marathon_results_2025;
```

You can also use `DATABASE_URL` instead of `SUPABASE_DB_URL`.

## Required columns
The dataset returned by `RESULTS_TABLE` / `RESULTS_QUERY` must include:

- `CountryOfResName`
- `CountryOfResAbbrev`
- `Gender`
