import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
import json
from datetime import datetime

# This is the code that is deployed on AWS Lambda to make an API call to the spotify API. Only here for sample/history/bugfixes.
def lambda_handler(event, context):
    CLIENT_ID = os.environ.get('CLIENT_ID')
    SECRET_ID = os.environ.get('CLIENT_SECRET')
    client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=SECRET_ID)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    top50_playlist_url = 'https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M'
    data = sp.playlist_tracks(top50_playlist_url)

    client = boto3.client('s3')
    file_name = f'spotify_raw_{datetime.now()}.json'
    
    
    client.put_object(Bucket='spotify-etl-pipeline-sn', Key=f'raw_data/to_be_processed/{file_name}', Body=json.dumps(data))