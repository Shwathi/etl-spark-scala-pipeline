# ETL Pipeline using Scala and Apache Spark

## Overview
This project demonstrates building an ETL pipeline using Apache Spark (Scala) to process sales data.

## Features
- Data extraction from CSV
- Data cleaning and transformation
- Aggregation (total sales, order count)
- Console output for validation

## Tech Stack
- Scala
- Apache Spark
- SBT

## Note
Due to Windows-specific Hadoop native library limitations, output is demonstrated using console logs. The pipeline is fully functional and can write to Parquet/CSV in a Linux environment.
