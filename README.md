
**Project Goal**
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


**Database Schema and ETL Pipeline**
The ETL data pipeline will process the data from s3://udacity automatically every time it runs.
It will populate the 5 parquet files: songs,songplays,artists, time, users

**Performance**
The spark to process log data will be faster but relatively slow in processing single files in multiple folders.
The enhancement for may be to create another script to combine all song json data into serval large json files
as log_data, and then have spark to process it.


**Instruction on Python scripts**
Import the etl.py file and run the main function:
import etl
etl.main()


**this will process the data file in aws s3 location and produce the parquet files**
etl.py
ETL script that process the json file and populate the parquet files.
