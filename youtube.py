import streamlit as st
import googleapiclient.discovery
import mysql.connector
import pandas as pd
from datetime import datetime

#----------------------

st.header("YOUTUBE DATA HARVESTING AND WAREHOUSUNG")

channel_id=st.text_input("Enter the channel ID")

#---------------

api_service_name = "youtube"
api_version = "v3"

api_key = "AIzaSyCWrl7yhUL8XIDP5k8FE7UNu3DCXCB-T90"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = api_key)


#----------------------------
if len(channel_id)==24:
    
    def get_channel_info(channel_id):
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id 
            )

        response = request.execute()

        for i in response["items"]:
            DATA = { 
                'Channel_Name' : i["snippet"]["title"],
                'Channel_ID' : i["id"],
                'Subscribers' : i["statistics"]["subscriberCount"],
                'Views' : i["statistics"]["viewCount"],
                'Total_videos' : i["statistics"]['videoCount'],
                'Channel_Description' : i["snippet"]["description"],
                'Playlist_ID' : i["contentDetails"]["relatedPlaylists"]["uploads"]

            }
            return pd.DataFrame([DATA])
    channel_details = get_channel_info(channel_id)
#    st.dataframe(channel_details)


#---------------------------


        #get video ids
    def get_videos_id(channel_id):  # channel id not assigned

        Videos_IDS = []

        request_channel_id = youtube.channels().list(
            part="contentDetails",
            id = channel_id
            )
        response_channel_id = request_channel_id.execute()

        channel_playlist_id = response_channel_id['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_tokens = None

        while True:
            request_playlist_id = youtube.playlistItems().list(
                part="snippet",
                playlistId=channel_playlist_id,
                maxResults = 50,
                pageToken = next_page_tokens
                )

            responce_playlist_id = request_playlist_id.execute()


            for i in range(len(responce_playlist_id['items'])):
                Videos_IDS.append(responce_playlist_id['items'][i]['snippet']['resourceId']['videoId'])
            next_page_tokens = responce_playlist_id.get('nextPageToken')

            if next_page_tokens is None:
                break
        
        return Videos_IDS

    Video_Ids = get_videos_id(channel_id)

#----------------------

        
    #get video information

    def get_video_info(Video_Ids):

        video_data = []

        for videosid in Video_Ids:
            request = youtube.videos().list(
                part ="snippet,contentDetails,statistics",
                id = videosid
            )

            response = request.execute()

            for info in response["items"]:

                pubished_at =datetime.strptime(info['snippet']['publishedAt'],"%Y-%m-%dT%H:%M:%SZ")

                DATA = {
                    'channel_name'          : info['snippet']['channelTitle'],
                    'channel_id'            : info['snippet']['channelId'],
                    'Video_Id'              : info['id'],
                    'Video_Title'           : info['snippet']['title'],
                    'Video_Description'     : info['snippet'].get('description'),
                    'Tags'                  : ','.join(info['snippet'].get('tags', [])),
                    'PublishedAt'           : pubished_at,
                    'View_Count'            : info['statistics'].get('viewCount'),
                    'Like_Count'            : info['statistics'].get('likeCount'),
                    'Favorite_Count'        : info['statistics']['favoriteCount'],
                    'Comment_Count'         : info['statistics'].get('commentCount'),
                    'Duration'              : info['contentDetails']['duration'],
                    'Thumbnail'             : info['snippet']['thumbnails']['default']['url'],
                    'Caption_Status'        : info['contentDetails']['caption']

                    }
                
                video_data.append(DATA)
                
        return pd.DataFrame(video_data)

    video_details = get_video_info(Video_Ids)
#    st.dataframe(video_details)

#----------------------------------

    #get comment information

    def get_comment_info(Video_ids):

        Comment_data = []
        try:
            for video_id in Video_ids:
                request = youtube.commentThreads().list(
                    part = "snippet",
                    videoId = video_id,
                    maxResults = 100
                )

                response = request.execute()

                for item in response['items']:

                    pubished_at =datetime.strptime(item['snippet']['topLevelComment']['snippet']['publishedAt'],"%Y-%m-%dT%H:%M:%SZ")

                    DATA = {

                        "channel_id" : item['snippet']['channelId'],
                        "Comment_ID" : item['snippet']['topLevelComment']['id'],
                        "Video_Id"   : item['snippet']['topLevelComment']['snippet']['videoId'],
                        "Comment_text"  : item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "Comment_Author"  : item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "Comment_published" :pubished_at
                    }

                    Comment_data.append(DATA)
        except:
            pass

        return pd.DataFrame(Comment_data)

    comment_details = get_comment_info(Video_Ids)
#    st.dataframe(comment_details)

#------------------------
    #get_playlist_details

    def get_playlist_info(channel_id):
        next_page_token = None
        playlist_data = []

        while True:
            request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults = 50,
                pageToken = next_page_token
            )
            response = request.execute()

            for i in response['items']:

                published_at = datetime.strptime(i['snippet']['publishedAt'],"%Y-%m-%dT%H:%M:%SZ")

                DATA = {
                    "Playlist_id" : i['id'],
                    "Playlist_Title"  : i['snippet']['title'],
                    "Channel_Id" : i['snippet']['channelId'],
                    "Channel_name" : i['snippet']['channelTitle'],
                    "playlist_published_date" : published_at,
                    "playlist_count" : i['contentDetails']['itemCount']
                }

                playlist_data.append(DATA)
            next_page_token= response.get('nextPageToken')

            if next_page_token is None:
                break
        return pd.DataFrame(playlist_data)

    playlist_details = get_playlist_info(channel_id)
#    st.dataframe(playlist_details)
#------------------------


# mysql connection
#    def channel_table():

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234"
    )
    mycursor = mydb.cursor()


    # Create database and use it
    mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
    mycursor.execute("USE youtube_db")

    # Create table query
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS channel(
        Channel_Name VARCHAR(255),
        Channel_ID VARCHAR(80) PRIMARY KEY,
        Subscribers BIGINT,
        Views BIGINT,
        Total_videos BIGINT,
        Channel_Description TEXT,
        Playlist_ID VARCHAR(80)
    )
    '''
    mycursor.execute(create_table_query)

    # Insert data into the table
    insert_query = '''
    INSERT INTO channel (
        Channel_Name,
        Channel_ID,
        Subscribers,
        Views,
        Total_videos,
        Channel_Description,
        Playlist_ID
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
     ON DUPLICATE KEY UPDATE 
        Channel_Name = VALUES(Channel_Name),
        Channel_ID = VALUES(Channel_ID),
        Subscribers = VALUES(Subscribers),
        Views = VALUES(Views),
        Total_videos = VALUES(Total_videos),
        Channel_Description = VALUES(Channel_Description),
        Playlist_ID = VALUES(Playlist_ID)     
     '''


    values = (
        channel_details['Channel_Name'][0],
        channel_details['Channel_ID'][0],
        channel_details['Subscribers'][0],
        channel_details['Views'][0],
        channel_details['Total_videos'][0],
        channel_details['Channel_Description'][0],
        channel_details['Playlist_ID'][0]
    )
    
    mycursor.execute(insert_query, values)
    mydb.commit()

    # Close the cursor and the connection
    if mycursor:
        mycursor.close()
    if mydb:
        mydb.close()

#-------------------------------

# mysql connection
#def playlist_table():


    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234"
    )
    mycursor = mydb.cursor()

    # Create database and use it
    mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
    mycursor.execute("USE youtube_db")

    # Create table query
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS Playlist(
        Playlist_id VARCHAR(80) PRIMARY KEY,
        Playlist_Title VARCHAR(255),
        Channel_Id VARCHAR(80),
        Channel_name VARCHAR(255),
        playlist_published_date TIMESTAMP,
        playlist_count INT
    )
    '''
    mycursor.execute(create_table_query)

    # Insert data into the table
    insert_query = '''
    INSERT INTO Playlist (
        Playlist_id,
        Playlist_Title,
        Channel_Id,
        Channel_name,
        playlist_published_date,
        playlist_count
    ) VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Playlist_Title = VALUES(Playlist_Title),
        Channel_Id = VALUES(Channel_Id),
        Channel_name = VALUES(Channel_name),
        playlist_published_date = VALUES(playlist_published_date),
        playlist_count = VALUES(playlist_count)
    '''

    # Iterate over the DataFrame rows and execute the insert query
    for index, row in playlist_details.iterrows():
        values = (
            row['Playlist_id'],
            row['Playlist_Title'],
            row['Channel_Id'],
            row['Channel_name'],
            row['playlist_published_date'],
            row['playlist_count']
        )
        mycursor.execute(insert_query, values)

    mydb.commit()

    # Close the cursor and the connection
    if mycursor:
        mycursor.close()
    if mydb:
        mydb.close()
        

#-------------------------

    # mysql connection
    #def videos_table():


    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234"
    )
    mycursor = mydb.cursor()

    # Create database and use it
    mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
    mycursor.execute("USE youtube_db")

    # Create table query
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS videos(
        channel_name VARCHAR(100),
        channel_id VARCHAR(100),
        Video_Id VARCHAR(80) PRIMARY KEY,
        Video_Title VARCHAR(255),
        Video_Description TEXT,
        Tags TEXT,
        PublishedAt TIMESTAMP,
        View_Count INT,
        Like_Count INT,
        Favorite_Count INT,
        Comment_Count INT,
        Duration VARCHAR(50),
        Thumbnail VARCHAR(255),
        Caption_Status VARCHAR(150)
    )
    '''
    mycursor.execute(create_table_query)


    for insert_query, row in video_details.iterrows():
        # Insert data into the table
        insert_query = '''
        INSERT INTO videos (
            channel_name,
            channel_id,
            Video_Id,
            Video_Title,
            Video_Description,
            Tags,
            PublishedAt,
            View_Count,
            Like_Count,
            Favorite_Count,
            Comment_Count,
            Duration,
            Thumbnail,
            Caption_Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Video_Title = VALUES(Video_Title),
        Tags = VALUES(Tags),
        PublishedAt = VALUES(PublishedAt),
        View_Count = VALUES(View_Count),
        Like_Count = VALUES(Like_Count),
        Favorite_Count = VALUES(Favorite_Count),
        Comment_Count = VALUES(Comment_Count),
        Duration = VALUES(Duration),
        Thumbnail = VALUES(Thumbnail),
        Caption_Status = VALUES(Caption_Status)                 
        '''

        values = (
            row['channel_name'],
            row['channel_id'],
            row['Video_Id'],
            row['Video_Title'],
            row['Video_Description'],
            row['Tags'],
            row['PublishedAt'],
            row['View_Count'],
            row['Like_Count'],
            row['Favorite_Count'],
            row['Comment_Count'],
            row['Duration'],
            row['Thumbnail'],
            row['Caption_Status']
        )

        mycursor.execute(insert_query, values)

    mydb.commit()

    # Close the cursor and the connection
    if mycursor:
        mycursor.close()
    if mydb:
        mydb.close()


#-------------------------


    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234"
    )

    mycursor = mydb.cursor()

    mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
    mycursor.execute("USE youtube_db")

    # Create comment table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS comment (
        channel_id VARCHAR(255),
        Comment_ID VARCHAR(255) PRIMARY KEY,
        Video_Id VARCHAR(255),
        Comment_text TEXT,
        Comment_Author VARCHAR(255),
        Comment_published TIMESTAMP
    )
    """
    mycursor.execute(create_table_query)

    # Insert data into the table
    insert_query = '''
    INSERT INTO comment (
        channel_id,
        Comment_ID,
        Video_Id,
        Comment_text,
        Comment_Author,
        Comment_published
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        Comment_text = VALUES(Comment_text),
        Comment_Author = VALUES(Comment_Author),
        Comment_published = VALUES(Comment_published)
    '''

    # Iterate over the DataFrame rows and execute the insert query
    for index, row in comment_details.iterrows():
        values = (
            row['channel_id'],
            row['Comment_ID'],
            row['Video_Id'],
            row['Comment_text'],
            row['Comment_Author'],
            row['Comment_published'].to_pydatetime()
        )
        mycursor.execute(insert_query, values)

    mydb.commit()

    # Close the cursor and the connection
    if mycursor:
        mycursor.close()
    if mydb:
        mydb.close()





else:
    pass



 #--------------------------
    # code for display table

    def mysql_connect():
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234",
            database = "youtube_db"

        )
        return connection

    def execute_query(query):
        connection = mysql_connect()
        cursour = connection.cursor()
        cursour.execute(query)
        column=[i[0] for i in cursour.description]
        data = cursour.fetchall()

        return pd.DataFrame(data,columns=column)

    channel = execute_query("SELECT * FROM channel")

    def execute_query(query):
        connection = mysql_connect()
        cursour = connection.cursor()
        cursour.execute(query)
        column=[i[0] for i in cursour.description]
        data = cursour.fetchall()

        return pd.DataFrame(data,columns=column)

    video = execute_query("SELECT * FROM videos")


    def execute_query(query):
        connection = mysql_connect()
        cursour = connection.cursor()
        cursour.execute(query)
        column=[i[0] for i in cursour.description]
        data = cursour.fetchall()

        return pd.DataFrame(data,columns=column)

    playlist = execute_query("SELECT * FROM playlist")

    def execute_query(query):
        connection = mysql_connect()
        cursour = connection.cursor()
        cursour.execute(query)
        column=[i[0] for i in cursour.description]
        data = cursour.fetchall()

        return pd.DataFrame(data,columns=column)

    comment = execute_query("SELECT * FROM comment")


    st.title(' channel')
    df_channel = pd.DataFrame(channel)
    df_video = pd.DataFrame(video)
    st.dataframe(df_channel)

    st.title(' videos')
    df_video = pd.DataFrame(video)
    st.dataframe(df_video)

    st.title(' Playlist')
    df_playlist = pd.DataFrame(playlist)
    st.dataframe(df_playlist)

    st.title(' comment')
    df_comment = pd.DataFrame(comment)
    st.dataframe(df_comment)

    #---------------------------------------------------



