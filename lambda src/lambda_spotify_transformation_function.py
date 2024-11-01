import json
import boto3
import pandas as pd
import io
from datetime import datetime

# This is the code that is deployed on AWS Lambda to grab all spotify api raw json data from the 'to be processed' s3 buckets, 
# transform, dump as csv to respective transformed bucket and further move the raw json to raw/processed and delete the raw unprocessed files.

def make_csv_buffer(df):
    # convert df to string for s3 stream to read.
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_content = csv_buffer.getvalue()
    return csv_content

def make_album(data):
    album_df_list = []
    for song in data['items']:
        album_id = song['track']['album']['id']
        album_name = song['track']['album']['name']
        release_date = song['track']['album']['release_date']
        total_tracks = song['track']['album']['total_tracks']
        album_url = song['track']['album']['external_urls']['spotify']
        row_data = [album_id, album_name, release_date, total_tracks, album_url]
        album_df_list.append(row_data)
    return album_df_list

def make_artist(data):
    artist_df_list = []
    for song in data['items']:
        artist_id = song['track']['artists'][0]['id']
        artist_name = song['track']['artists'][0]['name']
        artist_url = song['track']['artists'][0]['external_urls']['spotify']
        row_data = [artist_id, artist_name, artist_url]
        artist_df_list.append(row_data)
    return artist_df_list
    
def make_song(data):
    song_df_list = []
    for song in data['items']:
        song_id = song['track']['id']
        song_name = song['track']['name']
        duration_ms = song['track']['duration_ms']
        url = song['track']['external_urls']['spotify']
        popularity = song['track']['popularity']
        song_added = song['added_at']
        album_id = song['track']['album']['id']
        artist_id = song['track']['artists'][0]['id']
        row_data = [song_id, song_name, duration_ms, url, popularity, song_added, album_id, artist_id]
        song_df_list.append(row_data)
    return song_df_list

    
def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = 'spotify-etl-pipeline-sn'
    prefix_key = 'raw_data/to_be_processed/'
    
    spotify_data = []
    file_keys = []
    
    # grab all json files from the to be processed bucket 
    for file in s3_client.list_objects(Bucket=bucket_name, Prefix=prefix_key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            json_content = response['Body'].read()
            data = json.loads(json_content)
            
            spotify_data.append(data)
            file_keys.append(file_key)

    for data in spotify_data:
        album_df_list = make_album(data)
        artist_df_list = make_artist(data)
        song_df_list = make_song(data)

        song_df = pd.DataFrame(song_df_list, columns=['song_id', 'name', 'duration_ms', 'url', 'popularity', 'added_date', 'album_id', 'artist_id'])
        song_df['added_date'] = pd.to_datetime(song_df['added_date'])
        
        artist_df = pd.DataFrame(artist_df_list, columns=['artist_id', 'name', 'url'])
        artist_df.drop_duplicates(subset='artist_id', keep='first', inplace=True, ignore_index=True)
        
        album_df = pd.DataFrame(album_df_list, columns=['album_id', 'name', 'release_date', 'total_tracks', 'url'])
        album_df.drop_duplicates(subset='album_id', keep='first', inplace=True, ignore_index=True)
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
    
        song_key = f'transformed_data/song_data/song_transformed_{datetime.now()}.csv'
        album_key = f'transformed_data/album_data/album_transformed_{datetime.now()}.csv'
        artist_key = f'transformed_data/artist_data/artist_transformed_{datetime.now()}.csv'
        
        s3_client.put_object(Bucket=bucket_name, Key=song_key, Body=make_csv_buffer(song_df))
        s3_client.put_object(Bucket=bucket_name, Key=album_key, Body=make_csv_buffer(album_df))
        s3_client.put_object(Bucket=bucket_name, Key=artist_key, Body=make_csv_buffer(artist_df))
        
    
    # Move to be processed json files to processed bucket and delete to be processed files. 
    s3_resource = boto3.resource('s3')
    
    for each_key in file_keys:
        
        copy_source = {
            'Bucket': bucket_name,
            'Key': each_key 
        }
        s3_resource.meta.client.copy(copy_source, bucket_name, f'raw_data/processed/{each_key.split('/')[-1]}')
        s3_resource.Object(bucket_name, each_key).delete()