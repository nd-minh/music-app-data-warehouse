# Data Warehouse for a Music Streaming App

In this project, our aim is to design a data warehouse on AWS for a music streaming app. Our tasks are listed as follows:
1. Extract two sources of data stored on Amazon S3: logs of user activities on the app and song metadata
2. Stage the two data sources in Amazon Redshift
3. Transform data into a set of dimensional tables for analytics team

The two sources of data are described below.

- Source 1: Logs of user activity on the app, available in JSON format.
Example:

![alt text](/images/log-data.png "log-ex")

- Source 2: Metadata of available songs in the app, available also in JSON format.

Example:

```JSON
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

#### Staging Tables:
We will first stage data from source 1 and source 2 into 2 staging tables: `staging_events` and `staging_songs`, respectively. The staging tables have all the columns as in the data sources, with datatype of each column matches the content of that column. For more details on the datatypes of the columns, please refer to `sql_queries.py`.

### Schema Design:
We design a fact table and dimension tables so that it is optimized for queries on song play analysis. To that end, we propose a star schema and an ETL pipeline to transfer the data from the two staging tables into one fact tables and four dimension tables. The descriptions of the tables and their relationship is as follows.

#### Dimension Tables:
- Table `Artists`: contains information about artists in the app. *Columns:* artist_id, name, location, latitude, longitude. *Primary key (PK):* artist_id. 
- Table `Songs`: contains information about songs in the app. *Columns:* song_id, title, artist_id, year, duration. *PK:* song_id.
- Table `Users`: contains information about users in the app. *Columns:* user_id, first_name, last_name, gender, level. *PK:* user_id.
- Table `Time`: contains information about timestamps of records broken down into specific units. *Columns:* start_time, hour, day, week, month, year, weekday. *PK:* start_time.

#### Fact Table:
- Tables `SongPlays`: contains records in log data associated with page `NextSong`. *Columns:* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent. *PK:* songplay_id. *Foreign Keys(FK):* artist_id REFERENCES `Artists.artist_id`, song_id REFERENCES `Songs.song_id`, user_id REFERENCES `Users.user_id`, start_time REFERENCES `Time.start_time`. 

With this design, the analytics team can easily query songs that users are listening to, as well as related information about the users, artists, and temporal information of the listening sessions. The details of the extracting, staging, and transforming processes are documented in `etl.py`. Instruction to run the ETL pipeline is given belows.

### Build Instruction
1. Run `python create_tables.py` to create the staging tables and dimension tables.
2. Run `python etl.py` to run the ETL pipeline (extract data from S3, stage data into Redshift tables, insert into dimension tables).

**Appendix:** Configuration information to connect to Amazon S3 and Redshift is stored in `dwh.cfg`.  