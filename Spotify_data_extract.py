#!/usr/bin/env python
# coding: utf-8

# In[4]:


import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
def lambda_handler(event,context):
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    client_credentials_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp=spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    playlists=sp.user_playlists('spotify')
    playlist_link= "https://open.spotify.com/playlist/1llHjtjECBo12ChwOGe38L"
    playlist_URI = playlist_link.split("/")[-1]
    spotify_data = sp.playlist_tracks(playlist_URI)
    filename="spotify_raw_"+str(datetime.now())+".json"
    client=boto3.client('s3')
    client.put_object(
        Bucket="spotify-etl-project-samindla",
        Key="raw_data/to_processed/"+filename,
        Body=json.dumps(spotify_data)
    )
    glue=boto3.client('glue')
    gluejobname="spotify_transformation_job"
    try:
        runid=glue.start_job_run(JobName=gluejobname)
        status=glue.get_job_run(JobName=gluejobname,RunId=runid['JobRunId'])
        print("Job Status: ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)


# In[ ]:




