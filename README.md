# Purpose of this database
- Sparkify is a music streaming company. They have their raw user and song data stored in S3.
- The purpose of this project is to build an ETL pipeline to extract their data from S3, stages
them in Redshift, and transform data into structured datasets for analytics.
# State and justify database schema design and ETL pipeline
### Fact table: 
- songplays
### Dimension tables:
- users
- songs
- artists
- time
### Project template
- create_table.py: to create fact and dimension tables
- etl.py: file to load data from S3 to staging tables and process to fact/dimension tables
- sql_queries.py: store all SQL queries
