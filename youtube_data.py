import streamlit as st
import googleapiclient.discovery
import mysql.connector
import pandas as pd
from googleapiclient.errors import HttpError

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCJMlk5RiOJ9qa4nw5PTHPo17TxRqdkNjc"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

channel_id = "UCPyFYlqbkxkWX_dWCg0eekA"

def youtube_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    data = {
        'channel_id': channel_id,
        'channel_name': response['items'][0]['snippet']['title'],
        'channel_discription': response['items'][0]['snippet']['description'],
        'channel_published_at': response['items'][0]['snippet']['publishedAt'],
        'channel_playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'channel_subscribers_count': response['items'][0]['statistics']['subscriberCount'],
        'channel_video_count': response['items'][0]['statistics']['videoCount'],
        'channel_views_count': response['items'][0]['statistics']['viewCount']
    }
    
    return data

channel_detail = pd.DataFrame([youtube_channel_data(channel_id)])

def get_playlist_info(channel_id):
    next_page_token = None
    playlist_data = []

    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for i in response['items']:
            data = {
                "Playlist_id": i['id'],
                "Playlist_Title": i['snippet']['title'],
                "Channel_id": i['snippet']['channelId'],
                "Channel_name": i['snippet']['channelTitle'],
                "playlist_published_date": i['snippet']['publishedAt'],
                "playlist_count": i['contentDetails']['itemCount']
            }
            playlist_data.append(data)

        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    
    return pd.DataFrame(playlist_data)

playlist_data_details = get_playlist_info(channel_id)

def get_video_ids(channel_id):
    video_ids = []
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])

        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    
    return video_ids

video_ids = get_video_ids(channel_id)

def get_video_info(video_ids):
    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for info in response["items"]:
            data = {
                'channel_name': info['snippet']['channelTitle'],
                'channel_id': info['snippet']['channelId'],
                'Video_Id': info['id'],
                'Video_Title': info['snippet']['title'],
                'Video_Description': info['snippet'].get('description'),
                'PublishedAt': info['snippet']['publishedAt'],
                'View_Count': info['statistics'].get('viewCount'),
                'Like_Count': info['statistics'].get('likeCount'),
                'Favorite_Count': info['statistics']['favoriteCount'],
                'Comment_Count': info['statistics'].get('commentCount'),
                'Duration': info['contentDetails']['duration'],
                'Caption_Status': info['contentDetails']['caption']
            }
            video_data.append(data)

    return pd.DataFrame(video_data)

video_details = get_video_info(video_ids)

def get_comment_info(video_ids):
    Comment_data = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                DATA = {
                    "channel_id": item['snippet']['topLevelComment']['snippet']['authorChannelId']['value'],
                    "Comment_ID": item['snippet']['topLevelComment']['id'],
                    "Video_Id": item['snippet']['topLevelComment']['snippet']['videoId'],
                    "Comment_text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_published": item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                Comment_data.append(DATA)
        except HttpError as e:
            if e.resp.status == 403:
                st.warning(f"Comments are disabled for video ID {video_id}. Skipping...")
            else:
                st.error(f"An error occurred: {e}")
    return pd.DataFrame(Comment_data)

comment_details = get_comment_info(video_ids)

def iso_to_mysql_datetime(iso_date_str):
    # Convert ISO 8601 datetime string to MySQL DATETIME string
    return iso_date_str.replace('T', ' ').replace('Z', '')

def create_channel_db(channel_detail):
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234"
        )
        mycursor = mydb.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
        mycursor.execute("USE youtube_db")
        mycursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_information (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            channel_discription TEXT,
            channel_published_at DATETIME,
            channel_playlist_id VARCHAR(255),
            channel_subscribers_count INT,
            channel_video_count INT,
            channel_views_count INT
        )
        """)
        sql = """
        INSERT INTO channel_information (
            channel_id,
            channel_name,
            channel_discription,
            channel_published_at,
            channel_playlist_id,
            channel_subscribers_count,
            channel_video_count,
            channel_views_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            channel_name = VALUES(channel_name),
            channel_discription = VALUES(channel_discription),
            channel_published_at = VALUES(channel_published_at),
            channel_playlist_id = VALUES(channel_playlist_id),
            channel_subscribers_count = VALUES(channel_subscribers_count),
            channel_video_count = VALUES(channel_video_count),
            channel_views_count = VALUES(channel_views_count)
        """
        values = (
            channel_detail['channel_id'][0],
            channel_detail['channel_name'][0],
            channel_detail['channel_discription'][0],
            iso_to_mysql_datetime(channel_detail['channel_published_at'][0]),
            channel_detail['channel_playlist_id'][0],
            int(channel_detail['channel_subscribers_count'][0]),
            int(channel_detail['channel_video_count'][0]),
            int(channel_detail['channel_views_count'][0])
        )
        mycursor.execute(sql, values)
        mydb.commit()
    except mysql.connector.Error as e:
        st.error(f"Database error: {e}")
    finally:
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()

create_channel_db(channel_detail)

def create_video_db(video_details):
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234"
        )
        mycursor = mydb.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
        mycursor.execute("USE youtube_db")
        mycursor.execute("""
        CREATE TABLE IF NOT EXISTS video_information (
            channel_id VARCHAR(255),
            channel_name VARCHAR(255),
            Video_Id VARCHAR(255) PRIMARY KEY,
            Video_Title VARCHAR(255),
            Video_Description TEXT,
            PublishedAt DATETIME,
            View_Count INT,
            Like_Count INT,
            Favorite_Count INT,
            Comment_Count INT,
            Duration VARCHAR(255),
            Caption_Status VARCHAR(255)
        )
        """)
        sql = """
        INSERT INTO video_information (
            channel_id,
            channel_name,
            Video_Id,
            Video_Title,
            Video_Description,
            PublishedAt,
            View_Count,
            Like_Count,
            Favorite_Count,
            Comment_Count,
            Duration,
            Caption_Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            channel_name = VALUES(channel_name),
            Video_Title = VALUES(Video_Title),
            Video_Description = VALUES(Video_Description),
            PublishedAt = VALUES(PublishedAt),
            View_Count = VALUES(View_Count),
            Like_Count = VALUES(Like_Count),
            Favorite_Count = VALUES(Favorite_Count),
            Comment_Count = VALUES(Comment_Count),
            Duration = VALUES(Duration),
            Caption_Status = VALUES(Caption_Status)
        """
        for index, row in video_details.iterrows():
            values = (
                row['channel_id'],
                row['channel_name'],
                row['Video_Id'],
                row['Video_Title'],
                row.get('Video_Description'),
                iso_to_mysql_datetime(row['PublishedAt']),
                int(row['View_Count']) if row.get('View_Count') else None,
                int(row['Like_Count']) if row.get('Like_Count') else None,
                int(row['Favorite_Count']) if row.get('Favorite_Count') else None,
                int(row['Comment_Count']) if row.get('Comment_Count') else None,
                row['Duration'],
                row['Caption_Status']
            )
            mycursor.execute(sql, values)
        mydb.commit()
    except mysql.connector.Error as e:
        st.error(f"Database error: {e}")
    finally:
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()

create_video_db(video_details)

def create_playlist_db(playlist_data_details):
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234"
        )
        mycursor = mydb.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
        mycursor.execute("USE youtube_db")
        mycursor.execute("""
        CREATE TABLE IF NOT EXISTS playlist_information (
            Playlist_id VARCHAR(255) PRIMARY KEY,
            Playlist_Title VARCHAR(255),
            Channel_id VARCHAR(255),
            Channel_name VARCHAR(255),
            playlist_published_date DATETIME,
            playlist_count INT
        )
        """)
        sql = """
        INSERT INTO playlist_information (
            Playlist_id,
            Playlist_Title,
            Channel_id,
            Channel_name,
            playlist_published_date,
            playlist_count
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Playlist_Title = VALUES(Playlist_Title),
            Channel_id = VALUES(Channel_id),
            Channel_name = VALUES(Channel_name),
            playlist_published_date = VALUES(playlist_published_date),
            playlist_count = VALUES(playlist_count)
        """
        for index, row in playlist_data_details.iterrows():
            values = (
                row['Playlist_id'],
                row['Playlist_Title'],
                row['Channel_id'],
                row['Channel_name'],
                iso_to_mysql_datetime(row['playlist_published_date']),
                int(row['playlist_count'])
            )
            mycursor.execute(sql, values)
        mydb.commit()
    except mysql.connector.Error as e:
        st.error(f"Database error: {e}")
    finally:
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()

create_playlist_db(playlist_data_details)

def create_comment_db(comment_details):
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234"
        )
        mycursor = mydb.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
        mycursor.execute("USE youtube_db")
        mycursor.execute("""
        CREATE TABLE IF NOT EXISTS comment_information (
            channel_id VARCHAR(255),
            Comment_ID VARCHAR(255) PRIMARY KEY,
            Video_Id VARCHAR(255),
            Comment_text TEXT,
            Comment_Author VARCHAR(255),
            Comment_published DATETIME
        )
        """)
        sql = """
        INSERT INTO comment_information (
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
        """
        for index, row in comment_details.iterrows():
            values = (
                row['channel_id'],
                row['Comment_ID'],
                row['Video_Id'],
                row['Comment_text'],
                row['Comment_Author'],
                iso_to_mysql_datetime(row['Comment_published'])
            )
            mycursor.execute(sql, values)
        mydb.commit()
    except mysql.connector.Error as e:
        st.error(f"Database error: {e}")
    finally:
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()

create_comment_db(comment_details)


st.sidebar.header("YouTube Channel Data")
selected_page = st.sidebar.selectbox("Select a Page", ["Channel Details", "Playlist Details", "Video Details", "Comment Details"])

if selected_page == "Channel Details":
  channel_detail = pd.DataFrame([youtube_channel_data(channel_id)])
  st.header("Channel Details")
  st.dataframe(channel_detail)
elif selected_page == "Playlist Details":
  playlist_data_details = get_playlist_info(channel_id)
  st.header("Playlist Details")
  st.dataframe(playlist_data_details)
elif selected_page == "Video Details":
  video_ids = get_video_ids(channel_id)
  video_details = get_video_info(video_ids)
  st.header("Video Details")
  st.dataframe(video_details)
elif selected_page == "Comment Details":
  video_ids = get_video_ids(channel_id)
  comment_details = get_comment_info(video_ids)
  st.header("Comment Details")
  st.dataframe(comment_details)

# Execute database creation functions based on selected page
if selected_page == "Channel Details":
  create_channel_db(channel_detail)
elif selected_page == "Playlist Details":
  create_playlist_db(playlist_data_details)
elif selected_page == "Video Details":
  create_video_db(video_details)
elif selected_page == "Comment Details":
  create_comment_db(comment_details)


import streamlit as st
import mysql.connector
import pandas as pd

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="youtube_db"
    )

def execute_query(query):
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    connection.close()
    return pd.DataFrame(result, columns=columns)

# Define SQL Queries
queries = {
    "Video and Channel Names": "SELECT Video_Title, channel_name FROM video_information",
    "Most Videos by Channel": """
        SELECT channel_name, COUNT(Video_Id) as Video_Count
        FROM video_information
        GROUP BY channel_name
        ORDER BY Video_Count DESC
    """,
    "Top 10 Most Viewed Videos": """
        SELECT Video_Title, channel_name, View_Count
        FROM video_information
        ORDER BY View_Count DESC
        LIMIT 10
    """,
    "Comments per Video": """
        SELECT Video_Title, COUNT(Comment_ID) as Comment_Count
        FROM comment_information
        JOIN video_information ON comment_information.Video_Id = video_information.Video_Id
        GROUP BY Video_Title
    """,
    "Videos with Highest Likes": """
        SELECT Video_Title, channel_name, Like_Count
        FROM video_information
        ORDER BY Like_Count DESC
        LIMIT 10
    """,
    "Total Likes and Dislikes per Video": """
        SELECT Video_Title, channel_name, Like_Count, Dislike_Count
        FROM video_information
    """,
    "Total Views per Channel": """
        SELECT channel_name, SUM(View_Count) as Total_Views
        FROM video_information
        GROUP BY channel_name
    """,
    "Channels with Videos in 2022": """
        SELECT DISTINCT channel_name
        FROM video_information
        where extract(year from published_date)=2022
    """,
    "Average Duration per Channel": """
        SELECT channel_name, AVG(Duration) as Avg_Duration
        FROM video_information
        GROUP BY channel_name
    """,
    "Videos with Most Comments": """
        SELECT Video_Title, channel_name, Comment_Count
        FROM video_information
        ORDER BY Comment_Count DESC
        LIMIT 10
    """
}

st.sidebar.header("YouTube Data Queries")
selected_query = st.sidebar.selectbox("Select a Query", list(queries.keys()))

if selected_query:
    st.header(selected_query)
    query = queries[selected_query]
    result_df = execute_query(query)
    st.dataframe(result_df)
