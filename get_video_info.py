from youtube_transcript_api import YouTubeTranscriptApi
from sqlalchemy import create_engine
import mysql.connector as connection
import pandas as pd
import config
import scrapetube
import pandas as pd
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm

# ----------------This function is used to upload data to Mysql database.----------------------------------------
def data_to_sql(dataframe, delete_flag =False):
    try:
        host =      config.host
        user_id =   config.user_id
        password =  config.password
        database =  config.database

        mydb = connection.connect(host=host,database=database,user=user_id,password=password)
        cursor = mydb.cursor()
        engine=create_engine(f"mysql+pymysql://{user_id}:{password}@{host}/{database}")
        # to delete the records
        if delete_flag:
            query1 = "DELETE FROM transcripts"
            cursor.execute(query1)
            mydb.commit()
        # to read the records
        query = "SELECT * FROM transcripts;"
        df = pd.read_sql(query,mydb)
        print(df)
        # to insert the records into table
        # dataframe.to_sql(con=engine, name='transcripts', if_exists='append', index=False)
        
        mydb.close() #close the connection
    except Exception as e:
        mydb.close()
        print(str(e))

# -------------This function is used for getting the channel id for specific channel.----------------------------
def get_channel_id(channel_name):
    base_url = f'https://www.youtube.com/{channel_name}/'
    request = requests.get(base_url,verify=True)
    soup = BeautifulSoup(request.content,"html.parser")
    text = soup.prettify()
    text_list = text.split(',')
    text_list = [item.replace(r'"','') for item in text_list if 'externalId' in item]
    raw_id = ''.join(text_list)
    channel_id = raw_id.split(':')[1]
    print(channel_id)
    return channel_id

# ----------------this function is used for geting  all video in channel.---------------------------------------
def get_video_details(channel_name):
    videos = scrapetube.get_channel(channel_name)
    list_vid_id = []
    title_list = []
    url_list = []
    base_video_url = 'https://www.youtube.com/watch?v='
    for video in videos:
        # print(video)
        if len(video)!= 0:
            list_vid_id.append(video['videoId'])
            title_list.append(video['title']['runs'][0]['text'])
    if len(list_vid_id)!= 0:
        url_list = [base_video_url+id for id in list_vid_id if id!='']
    # print(url_list)
    df = pd.DataFrame(list(zip(title_list, url_list, list_vid_id)),columns =['Title', 'Video_url','Video_ID'])
    # df.to_excel('output.xlsx')
    print('video information extracted successfully.')
    return df

#--------------------- this function is to get trancscript from all videos available channel.----------------------
def get_transcript(video_id):
    text_list = []
    plain_text_list = []
    transcript = YouTubeTranscriptApi.get_transcript(video_id)
    for text in transcript:
        # text_list.append(f"{text['text']} start:{str(text['start'])}  duration:{str(text['duration'])}")
        plain_text_list.append(text['text'])
    # text = ', '.join(text_list)
    plain_text = ' '.join(plain_text_list)
    return plain_text

#-------------------- This is the final extraction function to combine all the process.---------------------------
def final_extract(channel_name, delete_flag =False):
    channel_id =  get_channel_id(channel_name)
    result = get_video_details(channel_id)
    print(f"total videos in {channel_name} are {len(result)}")
    result['Transcript'] = ''
    print("----------video transcript extraction started----------")

    for i in tqdm(range(result.shape[0]), desc = 'Processing...'):
        video_id = result['Video_ID'][i]
        try:
            transcript = get_transcript(video_id)
        except:
            transcript = 'uanble to extract since Subtitles are disabled for this video'
        result['Transcript'][i] = transcript

    result.to_excel('result.xlsx')
    # data = pd.read_excel('result.xlsx')
    data = result.copy()
    # data = data.head(5)
    data = data[['Video_url', 'Title','Video_ID', 'Transcript']]
    data.columns = ['url', 'title', 'videoid', 'transcript']
    data_to_sql(data, delete_flag)
if __name__ == "__main__":
    final_extract(config.channel_name)


