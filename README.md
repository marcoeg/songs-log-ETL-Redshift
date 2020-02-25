# Songs Log ETL Redshift
Sparkify app log data warehouse and ETL pipeline for songs analysis on Redshift.

### Abstract
The Sparkify mobile app is a music streaming application and is collecting users behavior as events in AWS S3. In preparation for analyzing these events in an efficient way, a Data Warehouse on AWS Redshift is created.

The events are stored in AWS S3 as JSON logs of user activity on the app, and metadata on songs available in the app are also available in an S3 bucket as JSON objects.

This project creates a Data Warehouse on Redshift with a schema designed to optimize queries for song play analysis and an ETL pipeline to populate the database.

#### Events Dataset 
The activity logs from the music streaming app are in log files in JSON format.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset:
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
File format:
```
{
    "artist":"Taylor Swift",
    "auth":"Logged In",
    "firstName":"Tegan",
    "gender":"F",
    "itemInSession":4,
    "lastName":"Levine",
    "length":238.99383,
    "level":"paid",
    "location":"Portland-South Portland, ME",
    "method":"PUT",
    "page":"NextSong",
    "registration":1540794356796.0,
    "sessionId":481,
    "song":"Cold As You",
    "status":200,
    "ts":1542061558796,
    "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId":"80"
}
```

The event logs are in a S3 bucket as specified in the `LOG_DATA` configuration variable in the `dwh.cfg` file. For convenience in populating a staging table in a single `COPY` command, the `LOG_JSONPATH` contains the URI of a file in json format containing all the log objects URIs.

### Songs Dataset
The dataset is a copy on AWS S3 of the Million Song Dataset http://millionsongdataset.com. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

For example, here are filepaths to two files in this dataset:
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

Below is the format of a single song file:
```
{
    "num_songs":1,
    "artist_id":"ARJIE2Y1187B994AB7",
    "artist_latitude":null,
    "artist_longitude":null,
    "artist_location":"",
    "artist_name":"Line Renaud",
    "song_id":"SOUPIRU12A6D4FA1E1",
    "title":"Der Kleine Dompfaff",
    "duration":152.92036,
    "year":0
}
```
The songs metadata files are in the S3 bucket specified in the `SONG_DATA` configuration variable in the `dwh.cfg` file.

#### Database for Song Play Analysis
The database schema is optimized for queries on song play analysis and it follows the `star` style. It includes the following tables:

##### Facts Table
`songplays` - records in log data associated with song plays

##### Dimension Tables
`users` - users in the app
`songs` - songs in music database
`artists` - artists in music database
`time` - timestamps of records in songplays broken down into specific units

##### Schema Diagram
![alt text](./img/sparkifydb-schema.png "schema")


>The database has been created to replicate the `users`, `artists` and `time` tables on all nodes and to distribute the `songs` and `songplays` tables across nodes using the `song_id` attribute to have stored on the same nodes events and songs in the event, and avoid shuffling in queries.

##### Hosting
AWS Redshift was choosen as the cloud technology to host the data warehouse as it uses parallel processing and multiple nodes to store data and run queries on multiple VPS in a transparent way.

#### ETL Process

The pipeline datasource are the JSON files containing the events and the songs stored in AWS S3 buckets. To optimize data insertion, the data extraction is in two stages:
1) The JSON files are copied from S3 buckets in two staging tables using bulk ingestion;
2) The data in the staging tables is transformed and loaded into the star schema database for analysis;

The ETL pipeline relies entirely on the AWS Redshift cluster and no additional resources need to be instantiated. The database created by the ETL pipeline can be accessed using traditional DB drivers and other AWS services for further data analysis and reporting.

1. Create the Redshift cluster running `utils/create_cluster.py`
2. Build the database running `create_tables.py`
3. Run the ETL process to load the database with `etl.py`
