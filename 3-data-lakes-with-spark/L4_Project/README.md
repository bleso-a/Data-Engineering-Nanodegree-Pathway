## Summary 

A music streaming startup, Sparkify need to analyse its grwoing database of the streaming app. 
The data currently resides in S3, in a directory of JSON logs on user activity
on the app. 

To allow Sparkify analyse the data, an ETL pipleline fro this. THe pipleine extracts data for storage on a data lake. 


## Running ETL Script 

```
python etl.py
```

## Files in Repository
* **[etl.py](etl.py)**: Script that extracts required information from logs stored in s3 buckets.
* **[dl.cfg](dl.cfg)**: Config file

## How to Run
Populate config file ```dl.cfg``` with AWS credentials
Run inside Spark master machine: ```python etl.py```


## Dataset
* **Song data**: ```s3://udacity-dend/song_data```
* **Log data**: ```s3://udacity-dend/log_data```

## Song ERD
![song_erd](img/Song_ERD.png)

* **Fact Table**: songplays
* **Dimension Tables**: users, songs, artists and time.

## Database Design

 
 #### Song Plays table

- *Type:* Fact table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `INTEGER` | The main identification of the table | 
| `start_time` | `TIMESTAMP` | The timestamp log |
| `user_id` | `INTEGER` | The user id that triggered this song play log.|
| `level` | `STRING` | The level of the user that triggered this song play log |
| `song_id` | `STRING` | The identification of the song that was played. |
| `artist_id` | `STRING` | The identification of the artist of the song. |
| `session_id` | `INTEGER` | The session_id of the user on the app |
| `location` | `STRING` | Song play log location |
| `user_agent` | `STRING` | The user agent of our app |

#### Users table

- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `user_id` | `INTEGER` | The main identification of an user |
| `first_name` | `STRING` | First name of the user.|
| `last_name` | `STRING` | Last name of the user. |
| `gender` | `STRING` | The gender is stated with just one character `M` (male) or `F` (female).|
| `level` | `STRING` | The level stands for the user app plans (`premium` or `free`) |


#### Songs table

- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `song_id` | `STRING` | The main identification of a song | 
| `title` | `STRING` | The title of the song.|
| `artist_id` | `STRING` | The artist id |
| `year` | `INTEGER` | The year that this song was made |
| `duration` | `DOUBLE` | The duration of the song |


#### Artists table

- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist_id` | `STRING` | The main identification of an artist |
| `name` | `STRING` | The name of the artist |
| `location` | `STRING` | The location where the artist are from |
| `latitude` | `DOUBLE` | The latitude of the location that the artist are from |
| `longitude` | `DOUBLE` | The longitude of the location that the artist are from |

#### Time table

- *Name:* `s3a://social-wiki-datalake/time.parquet`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `start_time` | `TIMESTAMP` | The timestamp|
| `hour` | `INTEGER` | The hour from the timestamp  |
| `day` | `INTEGER` | The day of the month from the timestamp |
| `week` | `INTEGER` | The week of the year from the timestamp |
| `month` | `INTEGER` | The month of the year from the timestamp |
| `year` | `INTEGER` | The year from the timestamp |
| `weekday` | `STRING` | The week day from the timestamp (Monday to Friday) |