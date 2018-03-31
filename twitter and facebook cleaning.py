#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 15:47:30 2018

@author: williamkye
"""
######################################
######## Trends Project ##############
######################################

#########IMPORT PACKAGES
import pandas as pd
import numpy as np
import collections
import re

#LOAD DATA
#2016 twitter data from scraping
twitter =pd.read_csv('twitter/twitter.csv')
fb =pd.read_csv('data/fb_trends.csv')
twitter_trends2016 = pd.read_pickle('data/twitter_2016')
fb2018 = pd.read_pickle('data/fb_2018')


#CLEAN DATA FROM TWITTER SCRAPE

#remove white spaces 
twitter['tweet'] = list(map(lambda x: x.strip(), twitter['tweet']))
#remove hashtags
twitter['tweet']= list(map(lambda x: x.strip('#'), twitter['tweet']))
#seperate words e.g. GoodFood
twitter['tweet_'] = list(map(lambda x: (re.sub(r"(\w)([A-Z])", r"\1_\2", x)), twitter['tweet']))
#remove space between tweets and add _
twitter['tweet_'] = list(map(lambda x: (re.sub(" ", "_", x)), twitter['tweet']))
#clean date so that only date number appears
twitter['date_'] = list(map(lambda x: re.sub('Trends for', '', x).strip(), twitter['date_']))

#CLEAN DATA FROM FACEBOOK TRENDS
#remove white spaces
fb['Fb'] = list(map(lambda x: x.strip(), fb['Fb']))
#remove hashtags
fb['Fb']= list(map(lambda x: x.strip('#'), fb['Fb']))
#space between tweets replaced with _
fb['Fb'] = list(map(lambda x: (re.sub(" ", "_", x)), fb['Fb']))
#hyphen replaced with _
fb['Fb'] = list(map(lambda x: (re.sub("-", "_", x)), fb['Fb']))
#commas removed
fb['Fb'] = list(map(lambda x: (re.sub(",", "", x)), fb['Fb']))
#make fb trends all lowercase
fb['Fb']=list(map(lambda x: x.lower(), fb['Fb']))

######## subsetting wiki data with twitter and facebook trends
#dates from twitter trends in 2016
date = list(set(twitter_trends2016['date_']))
#create list of wiki dates
wiki_date = ['jan'+ str(x) +'-' for x in range(8,15)]

#CLEAN DATA FROM TWITTER WIKI TRENDS
#create data from 2016 twitter trends
data=pd.DataFrame()
for i in range(0,7):
    twit = twitter_trends2016.loc[twitter_trends2016.date_==date[i]]
    twit = list(map(lambda x: '(' + x + ')$', twit['tweet_']))
    for j in range(0,24):
        wiki = pd.read_csv('data/'+wiki_date[i] + str(j), sep=' ', header=None, names=['proj', 'name', 'views', 'size'], skiprows= [4496772, 7085331, 5484987])
        wiki.dropna(subset =['name'], inplace = True)
        wiki= wiki[wiki['proj'].str.contains('en')]
        wiki= wiki[wiki['name'].str.match('|'.join(twit))]
        wiki = wiki.drop('proj', axis = 1)
        wiki= wiki.groupby('name')
        wiki=wiki.sum()
        wiki['day'] = date[i] + str(j)
        data = pd.concat([data, wiki])
        print (date[i] + str(j))
        #wiki['time'] = j
        #j = j+1
twitter_wiki = data

#CLEAN TWITTER DATA
twitter_wiki['hour']=list(map(lambda x: x[10:], twitter_wiki['day']))
missing = twitter_wiki['name'].value_counts()
missing
missing = missing[missing>17]
missing.keys()

sub= list(map(lambda x: x in missing.keys(), twitter_wiki['name']))
#sub1= list(map(lambda x: x.find('_')!=-1, twitter_wiki['name']))
twitter_wiki =twitter_wiki[sub]
#twitter_wiki = twitter_wiki[sub1]
twitter_wiki.drop('name', axis = 1, inplace = True)
twitter_wiki=twitter_wiki.reset_index()

twitter_wiki['period'] = [0 if int(x)<3 else
 1 if int(x) < 6 else
 2 if int(x) <9 else
 3 if int(x) < 13 else
 4 if int(x) <16 else
 5 if int(x)<19 else
 6 if int(x) <22 else
 7 for x in twitter_wiki['hour']]

twitter_wiki = twitter_wiki.groupby(['name', 'period'], group_keys = True).sum()
twitter_wiki = twitter_wiki.reset_index()

twitter_wiki.to_csv('data/twitter_wiki_r.csv')

###### cleaning for twitter

######### twitter data
twitter_wiki['hour']=list(map(lambda x: x[10:], twitter_wiki['day']))
missing = twitter_wiki['name'].value_counts()
missing
missing = missing[missing>17]
missing.keys()

sub= list(map(lambda x: x in missing.keys(), twitter_wiki['name']))
#sub1= list(map(lambda x: x.find('_')!=-1, twitter_wiki['name']))
twitter_wiki =twitter_wiki[sub]
#twitter_wiki = twitter_wiki[sub1]
twitter_wiki.drop('name', axis = 1, inplace = True)
twitter_wiki=twitter_wiki.reset_index()

twitter_wiki['period'] = [0 if int(x)<3 else
 1 if int(x) < 6 else
 2 if int(x) <9 else
 3 if int(x) < 13 else
 4 if int(x) <16 else
 5 if int(x)<19 else
 6 if int(x) <22 else
 7 for x in twitter_wiki['hour']]

twitter_wiki1 = twitter_wiki.groupby(['name', 'period'], group_keys = True).sum()
twitter_wiki1 = twitter_wiki1.reset_index()
twitter_wiki1.to_csv('data/twitter_wiki_r1.csv')

twitter_wiki1['group']= [0 if int(x)<16 else 1 for x in twitter_wiki1['hour']]
twitter_wiki1.to_csv('data/twitter_wiki_r2.csv')
names_t=pd.DataFrame(list(set(twitter_wiki1['name'])))
names_t.to_csv('data/names_t.csv')

cat1= pd.read_csv('data/names1.csv')
cat1.columns = ['index', 'name', 'cat']
#fb_wiki[list(map(lambda x: x not in ['alex_smith'], fb_wiki['name']))]
#fb_wiki1 =fb_wiki[fb_wiki['views']<5000]
twitter_wiki1 = pd.merge(twitter_wiki1, cat1, how = 'inner', on = 'name')
twitter_wiki1 = twitter_wiki1.groupby(['cat', 'period']).sum()
twitter_wiki1.reset_index(inplace=True)

twitter_wiki1.to_csv('data/twitter_wiki_r3.csv')


#create data from 2018 facebook trends

date = list(set(fb2018['date']))
print(date)
fbwiki_date = ['jan'+ str(x) +'-' for x in range(30,32)] +['feb'+ str(x) +'-' for x in range(1,6)]
print(fbwiki_date[1])

data=pd.DataFrame()
for i in range(0,7):
    fb2018 = fb2018.loc[fb2018.date==date[i]]
    fb = list(map(lambda x: '(' + x + ')$', fb2018['Fb']))
    for j in range(0,24):
        wiki = pd.read_csv('data/'+fbwiki_date[i] + str(j), sep=' ', header=None, names=['proj', 'name', 'views', 'size'], skiprows= [4942168])
        wiki['name'] = list(map(lambda x: str(x).lower(), wiki['name']))
        wiki['name'] = list(map(lambda x: (re.sub("-", "_", x)), wiki['name']))
        wiki = wiki.dropna(axis=0, how='any')
        wiki= wiki[wiki['proj'].str.contains('en')]
        wiki= wiki[wiki['name'].str.match('|'.join(fb))]
        wiki = wiki.drop('proj', axis = 1)
        wiki= wiki.groupby('name')
        wiki=wiki.sum()
        wiki['day'] = fbwiki_date[i] + str(j)
        data = pd.concat([data, wiki])
        print (fbwiki_date[i] + str(j))
        #wiki['time'] = j
        #j = j+1
fb_wiki = data



######### facebook data
fb_wiki = pd.read_pickle('data/fb_wiki')
fb_wiki['hour'] = list(map(lambda x: str(x)[-2:], fb_wiki['day']))
fb_wiki['hour'] = list(map(lambda x: re.sub('-','' ,x), fb_wiki['hour']))
fb_wiki['name']=fb_wiki.index

missing = fb_wiki['name'].value_counts()
missing = missing[missing>17]
missing.keys()

sub= list(map(lambda x: x in missing.keys(), fb_wiki['name']))
fb_wiki =fb_wiki[sub]
#sub1= list(map(lambda x: x.find('_')!=-1, fb_wiki['name']))
#fb_wiki = fb_wiki[sub1]
#fb_wiki
fb_wiki.drop('name', axis = 1, inplace = True)
fb_wiki=fb_wiki.reset_index()

fb_wiki['period'] = [0 if int(x)<=3 else
 1 if int(x) <=6 else
 2 if int(x) <=9 else
 3 if int(x) <=12 else
 4 if int(x) <=15 else
 5 if int(x)<=18 else
 6 if int(x) <=21 else
 7 for x in fb_wiki['hour']]

test = fb_wiki.groupby(['name', 'period'], group_keys = True).sum()
test = test.reset_index()

test.to_csv('data/fb_wiki_r1.csv')

fb_wiki['group']= [0 if int(x)<=12 else 1 for x in fb_wiki['hour']]
#names=pd.DataFrame(list(set(fb_wiki['name'])))
#names

#names.to_csv('data/names.csv')
fb_wiki.to_csv('data/fb_wiki_r2.csv')

#categorize data
cat.columns = ['index', 'name', 'cat']
#fb_wiki[list(map(lambda x: x not in ['alex_smith'], fb_wiki['name']))]
#fb_wiki1 =fb_wiki[fb_wiki['views']<5000]
fb_wiki1 = pd.merge(fb_wiki, cat, how = 'inner', on = 'name')
fb_wiki1 = fb_wiki1.groupby(['cat', 'period']).mean()
fb_wiki1.reset_index(inplace=True)

fb_wiki1.to_csv('data/fb_wiki_r3.csv')
#save as pickle file
twitter.to_pickle('data/twitter_2016')
fb.to_pickle('data/fb_2018')
twitter_wiki.to_pickle('data/twitter_wiki')
fb_wiki.to_pickle('data/fb_wiki')



