import streamlit as st
import mysql.connector
import pandas as pd

#---------------heading
st.header("Question and Answer")

#-----------sql connection
def sql_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="youtube_db"
    )
    return connection

def question_1():
    sql = "SELECT Video_Title, channel_name FROM videos"
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(sql)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['video_title', 'channel_name'])


    return pd_result

def question_2():
    qry = """
        SELECT channel_name, COUNT(Video_Id) as Video_Count
        FROM videos
        GROUP BY channel_name
        ORDER BY Video_Count DESC
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['channel_name', 'video_count'])

    return pd_result

def question_3():
    qry = """
        SELECT Video_Title, channel_name, View_Count
        FROM videos
        ORDER BY View_Count DESC
        LIMIT 10
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['Video_Title', 'channel_name','View_Count'])

    return pd_result


def question_4():
    qry = """
        SELECT Video_Title, COUNT(Comment_ID) as Comment_Count
        FROM comment
        JOIN videos ON comment.Video_Id = videos.Video_Id
        GROUP BY Video_Title
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['Video_Title', 'COUNT(Comment_ID)'])

    return pd_result


def question_5():
    qry = """
        SELECT Video_Title, channel_name, Like_Count
        FROM videos
        ORDER BY Like_Count DESC
        LIMIT 10
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['Video_Title', 'channel_name','Like_Count'])

    return pd_result


def question_6():
    qry = """
        SELECT Video_Title, channel_name, Like_Count
        FROM videos
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['Video_Title', 'channel_name','Like_Count'])

    return pd_result


def question_7():
    qry = """
        SELECT channel_name, SUM(View_Count) as Total_Views
        FROM videos
        GROUP BY channel_name
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['channel_name', 'Total_Views'])

    return pd_result


def question_8():
    qry = """
        SELECT DISTINCT channel_name
        FROM videos
        WHERE YEAR(PublishedAt) = 2022
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['channel_name'])

    return pd_result


def question_9():
    qry = """
        SELECT channel_name, AVG(Duration) as Avg_Duration
        FROM videos
        GROUP BY channel_name
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['channel_name', 'Avg_Duration'])

    return pd_result


def question_10():
    qry = """
        SELECT Video_Title, channel_name, Comment_Count
        FROM videos
        ORDER BY Comment_Count DESC
    """
    connect = sql_connection()
    mycursor = connect.cursor()
    mycursor.execute(qry)
    my_result = mycursor.fetchall()
    pd_result = pd.DataFrame(my_result, columns=['Video_Title', 'channel_name','Comment_Count'])

    return pd_result





Q_and_A = ["1.  What are the names of all the videos and their corresponding channels?",
           "2.  Which channels have the most number of videos, and how many videos do they have?",
           "3.  What are the top 10 most viewed videos and their respective channels?",
           "4.  How many comments were made on each video, and what are their corresponding video names?",
           "5.  Which videos have the highest number of likes, and what are their corresponding channel names?",
           "6.  What is the total number of likes for each video, and what are their corresponding video names?",
           "7.  What is the total number of views for each channel, and what are their corresponding channel names?",
           "8.  What are the names of all the channels that have published videos in the year 2022?",
           "9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?",
           "10. Which videos have the highest number of comments, and what are their corresponding channel names?"

]


selected_question = st.sidebar.selectbox("Select the question", Q_and_A)

#
qry = pd.DataFrame()


if selected_question == "1.  What are the names of all the videos and their corresponding channels?":
    st.subheader("1.  What are the names of all the videos and their corresponding channels?")
    qry = question_1()


elif selected_question == "2.  Which channels have the most number of videos, and how many videos do they have?":
    st.subheader("2.  Which channels have the most number of videos, and how many videos do they have?")
    qry = question_2()

elif selected_question == "3.  What are the top 10 most viewed videos and their respective channels?":
    st.subheader("3.  What are the top 10 most viewed videos and their respective channels?")
    qry = question_3()

elif selected_question == "4.  How many comments were made on each video, and what are their corresponding video names?":
    st.subheader("4.  How many comments were made on each video, and what are their corresponding video names?")
    qry = question_4()

elif selected_question == "5.  Which videos have the highest number of likes, and what are their corresponding channel names?":
    st.subheader("5.  Which videos have the highest number of likes, and what are their corresponding channel names?")
    qry = question_5()


elif selected_question == "6.  What is the total number of likes for each video, and what are their corresponding video names?":
    st.subheader("6.  What is the total number of likes and for each video, and what are their corresponding video names?")
    qry = question_6()


elif selected_question == "7.  What is the total number of views for each channel, and what are their corresponding channel names?":
    st.subheader("7.  What is the total number of views for each channel, and what are their corresponding channel names?")
    qry = question_7()

elif selected_question == "8.  What are the names of all the channels that have published videos in the year 2022?":
    st.subheader("8.  What are the names of all the channels that have published videos in the year 2022?")
    qry = question_8()

elif selected_question == "9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    st.subheader("9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?")
    qry = question_9()

elif selected_question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    st.subheader("10. Which videos have the highest number of comments, and what are their corresponding channel names?")
    qry = question_10()


# Display the dataframe
st.dataframe(qry)
