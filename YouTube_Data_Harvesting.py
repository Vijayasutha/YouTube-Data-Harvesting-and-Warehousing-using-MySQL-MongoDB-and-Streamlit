from  googleapiclient.discovery import build
#API Client library provides this functionality for all Google services. 

import pymongo
import pandas as pd
import mysql.connector
import streamlit as st

# API key connection
def API_connect():
    API_Id="AIzaSyAtp80k7pxfP8NYrMe1b3YnysaqsdfKB8U"
    API_service_name="youtube"
    API_version="v3"
    
    youtube=build(API_service_name,API_version,developerKey=API_Id)
    
    return youtube

youtube_data=API_connect()

#to get channel details

def get_channel_info(local_channel_id):

    request=youtube_data.channels().list(
        part="contentDetails,snippet,statistics",
        id=local_channel_id
    )

    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Subscribers_count=i['statistics']['subscriberCount'],
                No_of_views=i['statistics'][ 'viewCount'],
                Total_video_count=i['statistics'][ 'videoCount'],
                Channel_Id=i['id'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
                )
    return data



#to get video_ids

def get_video_ids(local_channel_id):
    video_ids=[]

    response=youtube_data.channels().list(
        id=local_channel_id,
        part='contentDetails'
        ).execute()

    #to get playlist id
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None #initially

    while True:
        response1=youtube_data.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,#id value is passed here
            maxResults=50,
            pageToken=next_page_token
            ).execute()


        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                                        #index-i is included here-->no.of videos
        next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break
        
    return video_ids 
       #appended ids will be returned



#get video information

def get_video_info(local_Video_Ids):
    video_data=[]
    for video_id in local_Video_Ids:
        request=youtube_data.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        
        for item in response['items']:
            data=dict(Channel_Name=item[ 'snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'], #id-->video_id                               
                    Title=item['snippet']['title'],
                    Tags= item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_at=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    View_count=item['statistics'].get('viewCount'),
                    Like_count=item['statistics'].get('likeCount'),                    
                    Dislike_count=item['statistics'].get('dislikeCount'),
                    Comment_count=item['statistics'].get('commentCount'),
                    Favorite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption=item['contentDetails']['caption']
                    )
            video_data.append(data)   
    return video_data
            

#get comment information

def get_comment_info(local_Video_Ids):
    Comment_data=[]

    try:
        for video_id in local_Video_Ids:
            request=youtube_data.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50 #50 comments at max
            )
                
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item[ 'snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author_Name=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published_at=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
            Comment_data.append(data)  
    except:
        pass 
    
    return Comment_data

#get_playlist_details

def get_Playlist_Details(local_channel_id):
    
    next_Page_Token=None
    Playlist_data=[]
    while True:
        request = youtube_data.playlists().list(
            part='snippet,contentDetails',
            channelId=local_channel_id,
            maxResults=50,#no.of playlists at max
            pageToken=next_Page_Token
                
        )    

        response = request.execute()

        for item in response['items']:
            data=dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                Published_at=item['snippet']['publishedAt'],
                Video_count=item['contentDetails']['itemCount']
            )
            Playlist_data.append(data)
            
        next_Page_Token=response.get('nextPageToken')
        
        if next_Page_Token is None:
            break
        
    return Playlist_data  

#upload to mongoDB

con=pymongo.MongoClient("mongodb://localhost:27017/") #27017 --> port for MongoDB server
db=con["Youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vid_ids=get_video_ids(channel_id)
    vid_details=get_video_info(vid_ids)
    com_details= get_comment_info(vid_ids)
    pl_details=get_Playlist_Details(channel_id)
    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "Video_information":vid_details,
                      "Comment_information":com_details,
                      "Playlist_information":pl_details
                      })

    return "All the data associated with the channel with id "+str(channel_id)+", has been successfully uploaded in to MongoDB!" 



# channels -table creation

def channels_table():

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456789",
        
        database='youtube_data',
        
        auth_plugin='mysql_native_password'
        )

    mycursor=mydb.cursor()
    

    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    
    # # Table creation

    try:
        create_query='''create table if not exists channels(
            Channel_Name varchar(100) primary key,
                                # primary key-->uniquely identifies each record in a table. 
                                # must contain UNIQUE values, 
                                # and cannot contain NULL values.
            Subscribers_count bigint,
            No_of_views bigint,
            Total_video_count int,
            Channel_Id varchar(80),
            Channel_Description text,
            Playlist_Id varchar(80)
            )'''
        mycursor.execute(create_query)
        mydb.commit()
        
    except:
        print("channels table already created")
        
    
    #to convert the extracted data into dataframe

    ch_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                            #{}curly braces empty--> all the channels
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
            Subscribers_count,
            No_of_views,
            Total_video_count,
            Channel_Id,
            Channel_Description,
            Playlist_Id)
            
            values(%s,%s,%s,%s,%s,%s,%s
            )'''
        values=(row['Channel_Name'],
                row['Subscribers_count'],
                row['No_of_views'],
                row['Total_video_count'],
                row['Channel_Id'],
                row['Channel_Description'],
                row['Playlist_Id'],
                )
    
        try:
            mycursor.execute(insert_query,values)
            mydb.commit() 

        except:
            print("Channels values are already inserted")
    

# playlists - table creation

def playlists_table():
    
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456789",        
        database='youtube_data',        
        auth_plugin='mysql_native_password',
        charset="utf8mb4"
        )

    mycursor=mydb.cursor()


    drop_query='''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()


    # # Table creation

    create_query='''create table if not exists playlists(
        Playlist_Id varchar(100) primary key,                  
        Title varchar(100),
        Channel_Id varchar(100),
        Channel_Name varchar(100),
        Published_at varchar(100),
        Video_count int
        )'''
    mycursor.execute(create_query)
    mydb.commit()
    
    #to convert the extracted data into dataframe

    pl_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]

    for pl_data in coll1.find({},{"_id":0,"Playlist_information":1}):
        for i in range(len(pl_data["Playlist_information"])):
            pl_list.append(pl_data["Playlist_information"][i])
    df1=pd.DataFrame(pl_list)


    for index,row in df1.iterrows():
        insert_query='''insert into playlists(
            Playlist_Id,                  
            Title,
            Channel_Id,
            Channel_Name,
            Published_at,
            Video_count)
            
            values(%s,%s,%s,%s,%s,%s)
            '''
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Published_at'],
                row['Video_count'],
                )
    
      
        mycursor.execute(insert_query,values)
        mydb.commit() 


def videos_table():

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456789",        
        database='youtube_data',        
        auth_plugin='mysql_native_password',
        charset="utf8mb4"
        )

    mycursor=mydb.cursor()


    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()


    # # Table creation

    create_query='''create table if not exists videos(  Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(13) PRIMARY KEY,                               
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                                        Published_at varchar(100),
                                                        Duration varchar(100),
                                                        View_count bigint,
                                                        Like_count bigint,                                                        
                                                        Dislike_count bigint,
                                                        Comment_count int,
                                                        Favorite_count int,
                                                        Definition varchar(10),
                                                        Caption varchar(50)
                                                        )'''
    mycursor.execute(create_query)
    mydb.commit()
    
    vi_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]

    for vi_data in coll1.find({},{"_id":0,"Video_information":1}):
        for i in range(len(vi_data["Video_information"])):
            vi_list.append(vi_data["Video_information"][i])
    df_Videos=pd.DataFrame(vi_list)
    
    for index,row in df_Videos.iterrows():
        tag_str= ''
        if (row['Tags']):
            for e in row['Tags']:
                tag_str += '#'
                tag_str += e
                tag_str += ', '
        
        # print (row['Description'].encode().decode())

        insert_query='''insert into videos( Channel_Name,
                                            Channel_Id,
                                            Video_Id,                               
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_at,
                                            Duration,
                                            View_count,
                                            Like_count,
                                            Dislike_count,
                                            Comment_count,
                                            Favorite_count,
                                            Definition,
                                            Caption
                                            )        
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            '''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                tag_str,
                row['Thumbnail'],
                row['Description'].encode().decode(),
                row['Published_at'],
                row['Duration'],
                row['View_count'],
                row['Like_count'],
                row['Dislike_count'],
                row['Comment_count'],
                row['Favorite_count'],
                row['Definition'],
                row['Caption'],
                )
        mycursor.execute(insert_query,values)
        mydb.commit()
    
def comments_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456789",        
        database='youtube_data',        
        auth_plugin='mysql_native_password',
        charset="utf8mb4"
        )

    mycursor=mydb.cursor()


    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()


    # # Table creation

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author_Name varchar(150),
                                                        Comment_Published_at varchar(50)
                                                        )'''
    mycursor.execute(create_query)
    mydb.commit()
    
    comments_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]

    for comments_data in coll1.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(comments_data["Comment_information"])):
            comments_list.append(comments_data["Comment_information"][i])
    df_Comments=pd.DataFrame(comments_list)
    
    for index, row in df_Comments.iterrows():
        insert_query='''insert into comments( Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author_Name,
                                            Comment_Published_at
                                            )        
            values(%s,%s,%s,%s,%s)
            '''
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author_Name'],
                row['Comment_Published_at']
                )
        mycursor.execute(insert_query,values)
        mydb.commit()


def Mongodb_to_MySQL():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Migration Completed Successfully!"

def Show_Channels_Table():
    ch_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    return(st.dataframe(ch_list))

def Show_Playlists_Table():
    pl_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]

    for pl_data in coll1.find({},{"_id":0,"Playlist_information":1}):
        for i in range(len(pl_data["Playlist_information"])):
            pl_list.append(pl_data["Playlist_information"][i])
    return(st.dataframe(pl_list))

def Show_Videos_Table():
    vi_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]

    for vi_data in coll1.find({},{"_id":0,"Video_information":1}):
        for i in range(len(vi_data["Video_information"])):
            vi_list.append(vi_data["Video_information"][i])
    return(st.dataframe(vi_list))

def Show_Comments_Table():
    comments_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]

    for comments_data in coll1.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(comments_data["Comment_information"])):
            comments_list.append(comments_data["Comment_information"][i])
    return(st.dataframe(comments_list))


with st.sidebar:
    st.title(":green[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")
    st.header("Problem Statement")
    st.subheader('''The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:''')
    st.caption('''1. Ability to input a YouTube channel ID and retrieve all the relevant data
(Channel name, subscribers, total video count, playlist ID, video ID, likes,
dislikes, comments of each video) using Google API.''')
    st.caption('''2. Option to store the data in a MongoDB database as a data lake.''')
    st.caption('''3. Ability to collect data for up to 10 different YouTube channels and store them in
the data lake by clicking a button.''')
    st.caption('''4. Option to select a channel name and migrate its data from the data lake to a
SQL database as tables.''')
    st.caption('''5. Ability to search and retrieve data from the SQL database using different
search options, including joining tables to get channel details.''')
    
channel_id = st.text_input("Enter the Channel ID")
if st.button("Get Data"):
    ch_id_list = []
    db=con["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_id_list.append(ch_data['channel_information']['Channel_Id'])
        
    if channel_id in ch_id_list:
        st.warning("The entered Channel ID information is already available in database")
        
    else:
        load_data = channel_details(channel_id)
        st.success(load_data)
        
if st.button("Migrate the data to MySQL"):
    migration = Mongodb_to_MySQL()
    st.success(migration)
    
on = st.toggle('Drop Down List')

# select_table = st.radio("Kindly select the table you want to view",("Channels", "Playlists", "Videos", "Comments"))

if on:
    select_table = st.selectbox("Kindly select the table", ("Channels", "Playlists", "Videos", "Comments"))
    
else:
    select_table = st.radio("Kindly select the table you want to view",("Channels", "Playlists", "Videos", "Comments"))

# select_table = st.slider('Kindly select the table', 1,2,3)

if (select_table == "Channels"):
    Show_Channels_Table()

if (select_table == "Playlists"):
    Show_Playlists_Table()
    
if (select_table == "Videos"):
    Show_Videos_Table()

if (select_table == "Comments"):
    Show_Comments_Table()



question = st.selectbox("Please choose your question",('1. What are the names of all the videos and their corresponding channels?',
                                                       '2. Which channels have the most number of videos, and how many videos do they have?',
                                                       '3. What are the top 10 most viewed videos and their respective channels?',
                                                       '4. How many comments were made on each video, and what are their corresponding video names?',
                                                       '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                       '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                                       '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                                       '8. What are the names of all the channels that have published videos in the year 2022?',
                                                       '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                                       '10. Which videos have the highest number of comments, and what are their corresponding channel names?',
                                                       ))    
questions_db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456789",        
        database='youtube_data',        
        auth_plugin='mysql_native_password',
        charset="utf8mb4"
        )

questions_cursor=questions_db.cursor(buffered=True)

if question == "1. What are the names of all the videos and their corresponding channels?":
    query = '''select Title as videos, Channel_Name as channelname from videos'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Videos', 'Channel Name'])
    st.write(question_df)
    
elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
    query = '''select Channel_Name as channelname, Total_video_count as no_videos from channels order by Total_video_count desc'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Channel Name', 'Total Number of Videos'])
    st.write(question_df)
    
elif question == "3. What are the top 10 most viewed videos and their respective channels?":
    ch_list=[]
    db=con["Youtube_data"]
    coll1=db["channel_details"]
    count = 0
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            count += 1
    query = '''select View_count as views, Channel_Name as channelname, Title as videotitle from videos order by View_count desc limit '''+str(count)
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['View Count', 'Channel Name', 'Video Name'])
    st.write(question_df)
    
elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
    query = '''select Title as videos, Comment_count as comments from videos order by Comment_count desc'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Video', 'Total Number of Comments'])
    st.write(question_df)
    
elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query = '''select Title as videos, Like_count as likes from videos order by Like_count desc'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Video', 'Total Number of Likes'])
    st.write(question_df)
    
elif question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query = '''select Title as videos, Like_count as likes, Dislike_count as dislikes from videos'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Video', 'Total Number of Likes', 'Total Number of Dislikes'])
    st.write(question_df)
    
elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query = '''select Channel_Name as channelname, No_of_views as total_views from channels'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Channel Name', 'Number of Views'])
    st.write(question_df)
    
elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
    query = '''select DISTINCT Channel_Name from videos where SUBSTRING(Published_at,1,4) = "2022"'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Channel Name'])
    st.write(question_df)
    
elif question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query = '''select Channel_name as channelname, AVG(CAST(Duration AS UNSIGNED)) as average_duration from videos group by Channel_name'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Channel Name', 'Average Video Length'])
    st.write(question_df)
    
elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query = '''select Channel_name as channelname, Title as title, Comment_count as comment_count from videos order by Comment_count desc'''
    questions_cursor.execute(query)
    questions_db.commit()
    result = questions_cursor.fetchall()
    question_df = pd.DataFrame(result, columns = ['Channel Name', 'Video Title', 'Average Video Length'])
    st.write(question_df)