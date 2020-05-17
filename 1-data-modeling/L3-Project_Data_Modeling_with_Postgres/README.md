# Running Sparkify song play logs ETL process

Using the song and log datasets, this project extract, transform and load data from the Sparkify music application logs.
 - `songplays`
 - `users`
 - `songs`
 - `artists`
 - `time` 



The projects require a star schema to be created for queries on song play as listed above. The first step is to create the Database. 

## Overview
- `Create Tables`
- `Build ETL Processes`
- `Build ETL Pipeline`



## Database Schema


## Project ETL Process

The first step is to create a Postgres Database :

```
python create_tables.py
```

Next is to parse the logs files - This perform ETL on the data and load it into the DB

```
python etl.py
```

Finally, test with test.ipynb to ensure the data was loaded correctly


## Database Star Schema Design

Details about the tables in the Schema

### Song Plays table

- *Name:* `songplays`
- *Type:* Fact table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `INTEGER` | Identification details of the table | 
| `start_time` | `TIMESTAMP NOT NULL` | TImestamp from log |
| `user_id` | `INTEGER NOT NULL REFERENCES users (user_id)` | User identification details |
| `level` | `VARCHAR` | Song play level |
| `song_id` | `VARCHAR REFERENCES songs (song_id)` | The identification of the song that was played. |
| `artist_id` | `VARCHAR REFERENCES artists (artist_id)` | Identification details of the artist |
| `session_id` | `INTEGER NOT NULL` | The session_id of the user details |
| `location` | `VARCHAR` | Location for the song |
| `user_agent` | `VARCHAR` |USer Agent of the song |

### Users table

- *Name:* `users`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `user_id` | `INTEGER PRIMARY KEY` | The main identification of an user |
| `first_name` | `VARCHAR NOT NULL` | First name of the user. |
| `last_name` | `VARCHAR NOT NULL` | Last name of the user. |
| `gender` | `CHAR(1)` | The gender is stated with just one character `M` (male) or `F` (female). Otherwise it can be stated as `NULL` |
| `level` | `VARCHAR NOT NULL` | The level stands for the user app plans (`premium` or `free`) |


### Songs table

- *Name:* `songs`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `song_id` | `VARCHAR PRIMARY KEY` | The main identification of a song | 
| `title` | `VARCHAR NOT NULL` | The title of the song. It can not be null, as it is the basic information we have about a song. |
| `artist_id` | `VARCHAR NOT NULL REFERENCES artists (artist_id)` | The artist id, it can not be null as we don't have songs without an artist, and this field also references the artists table. |
| `year` | `INTEGER NOT NULL` | The year that this song was made |
| `duration` | `NUMERIC (15, 5) NOT NULL` | The duration of the song |


### Artists table

- *Name:* `artists`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist_id` | `VARCHAR PRIMARY KEY` | The main identification of an artist |
| `name` | `VARCHAR NOT NULL` | The name of the artist |
| `location` | `VARCHAR` | The location where the artist are from |
| `latitude` | `NUMERIC` | The latitude of the location that the artist are from |
| `longitude` | `NUMERIC` | The longitude of the location that the artist are from |

### Time table

- *Name:* `time`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `start_time` | `TIMESTAMP NOT NULL PRIMARY KEY` | The timestamp itself, serves as the main identification of this table |
| `hour` | `NUMERIC NOT NULL` | The hour from the timestamp  |
| `day` | `NUMERIC NOT NULL` | The day of the month from the timestamp |
| `week` | `NUMERIC NOT NULL` | The week of the year from the timestamp |
| `month` | `NUMERIC NOT NULL` | The month of the year from the timestamp |
| `year` | `NUMERIC NOT NULL` | The year from the timestamp |
| `weekday` | `NUMERIC NOT NULL` | The week day from the timestamp |

