'''
Created on 04-Feb-2019

@author: Teerta shetty
'''

import csv
import spacy
import tweepy
import re
import pandas as pd
import pymysql


from time import sleep
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import vaderSentiment.vaderSentiment as vs
import emoji

connection = pymysql.connect(
         host='localhost',
         user='root',
         password='',                             
         db='analyticsproj',
         port=3306
         )
cursor2=connection.cursor()
cursor3=connection.cursor()
####input your credentials here
consumer_key = 'wlFhHiZJSTVb3k1rcjTqx65Ch' 
consumer_secret = 'Bdo5g3DRequWoK8HgaQMf9EXYDEEOPDScsisDXPnoZiWIdKb1g'
access_token = '1082646302301790208-SpvxhYpwjVsYAFaQZwVSNlNMPFWuQB'
access_token_secret = 'auTrniTix2biWt31zFPjz82P6GWadyA77WcJEg9NVAR62'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
 
def extract_emojis(strr):
    myemoji = emoji
    typeofem = type(myemoji)
    if(str(typeofem) == "<class 'module'>"):
        myemoji = emoji.UNICODE_EMOJI
    return ''.join(c for c in strr if c in myemoji)

analyser = SentimentIntensityAnalyzer()
 
def sentiment_analyzer_scores(sentence):
    score = analyser.polarity_scores(sentence)
    print(sentence, "------", str(score))
    return score

tmp=pd.DataFrame()

j=0      



sql2="select count(`keyword_name`) from `keywords_master`"
cursor3.execute(sql2)

sql="select `keyword_id`,`keyword_name`,`language` from `keywords_master`" 
 
cursor2.execute(sql)

for x in cursor3:
    
    records_count= x[0]
 
arr = []

for i in range(0,records_count):
   
    for t in cursor2:
        print ("=====================")
        arr.append(t)
        
count_arr = len(arr)
for i in range(count_arr):
    keyword_id = arr[i][0]
    keyword = arr[i][1]
    language = arr[i][2]
    
    print (keyword)
    
    for tweet in tweepy.Cursor(api.search,q=keyword,count=2000,
                               lang="en",tweet_mode='extended').items(2):
        if(j>=12000):
            print('limit reached sleeping now')
            sleep(15*60)
            j=0  
        print("=====================")  
        print (tweet.created_at, tweet.full_text)
        print("=====================") 
        print(pd.DataFrame(data=[extract_emojis(tweet.full_text)]))
        print("=====================") 
        tmp=tmp.append(pd.DataFrame(data=[extract_emojis(tweet.full_text)]),ignore_index=True)
        j=j+1
        print ("j: ",j)
    
    print("Printing tmp")
    print(tmp)
    
    
    
    emoji={}    
    emoji=analyser.make_emoji_dict()
    emojiDF=pd.DataFrame(list(emoji.items()))
    
    emojiName=pd.DataFrame()
    
    emojiCountList=pd.DataFrame()
    
    for j in range(0,len(emojiDF)):
        emojiCountList.loc[j,0] = emojiDF.loc[j,0]
        emojiCountList.loc[j,1] = 0
        
    print("this is emojiCountList")
    print(emojiCountList)
    
    for i in range(0,len(tmp)):
        print(i)
        for x in range(0,len(emojiCountList)):
            for emo in tmp.loc[i,0]: 
                if (emo == emojiCountList.loc[x,0]):
                    print("emo",emo)
                    sentiment_analyzer_scores(emo)
                    emojiCountList.loc[x,1]=emojiCountList.loc[x,1] + 1
                    
                    emoji_array=emojiCountList.loc[x]
                    emoji_sym = emoji_array[0]
                    emoji_count = emoji_array[1]
                    
                    print ("_____________111111111_______________")
                    print (emoji_sym)
                    
                    cursor=connection.cursor()
                    sql="INSERT INTO emoji(emoji,fo_key_id,count) VALUES ('"+str(emoji_sym)+"','"+str(keyword_id)+"','"+str(emoji_count)+"')"
                    cursor.execute(sql)
                    connection.commit()
                
                
                







         
#print(emojiName.groupby(0).count())

# countArray=pd.DataFrame(data=[['sample',0],['sample',0]],columns=['emoji','count'])
# for w1 in range (0,len(emojiName)):
#     flag=0
#     for w2 in range (0,len(countArray)):
#         if(emojiName.loc[w1,0] == countArray.loc[w2,0]):
#             countArray.loc[w2,1]=countArray.loc[w2,1] + 1 
#             flag=1
#     if(flag==0):
#         countArray=countArray.append([emojiName.loc[w1,0],1])
#         print(countArray)
                
#print(countArray)