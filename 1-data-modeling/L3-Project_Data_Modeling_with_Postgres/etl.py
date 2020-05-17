import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np




def process_song_file(cur, filepath):
    """
    Docstring
    Process songs log file
    Inputs:
     :param cur: the cursor object
     :param filepath: log data file path
     :return: None
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    #cur is the cursor object
    #insert_df is the inser generator function
    
    #define denerator function 

def insert_df( df, insert_query):
    """
    Docstring- Generator function
    insert dataframe with insert query parsed
    Inputs:
    :param df: Pandas DataFrame
    :param insert_query: Given Insert Query
    
    """
    for i, row in df.iterrows():
        cur.execute(insert_query, list(row))

    
    song_data =  df[['song_id','title', 'artist_id', 'year', 'duration']]
    song_data = song_data.drop_duplicates()
    song_data = song_data.replace(np.nan, None, regex = True)
    insert_df(cur, song_data, song_table_insert)
    #cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =  df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.drop_duplicates()
    artist_data = artist_data.replace(np.nan, None, regex=True)
    insert_df(artist_data, artist_table_insert)
    #cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Docstring
    Process songplay log file
    Inputs:
     :param cur: the cursor object
     :param filepath: log data file path
     :return: None
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.DataFrame({
        'start_time': pd.to_datetime(df['ts'], unit='ms')
    })
    
    #Convert to time metrics and create new columns
    
    t['hour'] = t['start_time'].dt.hour
    t['day'] = t['start_time'].dt.day
    t['week'] = t['start_time'].dt.week
    t['month'] = t['start_time'].dt.month
    t['year'] = t['start_time'].dt.year
    t['weekday'] = t['start_time'].dt.weekday
    
    #Drop duplicates before calling insert function 
    
    t = t.drop_duplicates()
    
    
    # insert time data records
    insert_df(t, time_table_insert)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # DRop Duplicates
    user_df = user_df.drop_duplicates()
    user_df = user_df.replace(np.nan, None, regex=True)
    user_df.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    
    
    # insert user records
    insert_df(user_df, user_table_insert)


    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
           index, pd.to_datetime(row.ts, unit='ms'),
        row.userId, row.level, songid, artistid,
        row.sessionId, row.location, row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Docstring
    Process all the data gotten from the json file path, and execute functions
    Inputs:
     :param cur: the cursor object
     :param conn: Establish postgres connection
     :param filepath: log data file path
     :param func: The function to process one log per time
     :return: None
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Docstring
    The main function
    No Input
     :return: None
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()