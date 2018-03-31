#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 16:18:21 2018

@author: williamkye
"""

######## Scraping data from wiki##############
###########import packages
import urllib.request
import gzip
import os

############# import data from 2016 wiki pages
#create list of urls
url =['https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-01/pagecounts-201601' + str(x).zfill(2) + '-' for x in range(8,16)]
#emply list for appending
url1 =[]
#
for i in url:
    for j in range(0,24):
        url1.append(i + str(j).zfill(2) + '0000' + '.gz')
filename =[]      
for i in range(8,16):
    for j in range(0,24):
        filename.append('jan' + str(i) + '-' + str(j)) 
        
# loop to download data    
for i in range(0,192):
    urllib.request.urlretrieve(url1[i], '/Users/williamkye/Box Sync/nyc data science academy/project_2/data/' + filename[i] + '.gz')
    print('completed:' + str(i))

############# import data from 2018 wiki pages
url =['https://dumps.wikimedia.org/other/pageviews/2018/2018-01/pageviews-201801' + str(x).zfill(2) + '-' for x in range(30,32)]
url1 =[]

for i in url:
    for j in range(0,24):
        url1.append(i + str(j).zfill(2) + '0000' + '.gz')
url =['https://dumps.wikimedia.org/other/pageviews/2018/2018-02/pageviews-201802' + str(x).zfill(2) + '-' for x in range(1,6)]
for i in url:
    for j in range(0,24):
        url1.append(i + str(j).zfill(2) + '0000' + '.gz')
filename =[]      
for i in range(30,32):
    for j in range(0,24):
        filename.append('jan' + str(i) + '-' + str(j)) 
for i in range(1,6):
    for j in range(0,24):
        filename.append('feb' + str(i) + '-' + str(j)) 

#url = 'https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-01/pagecounts-20160101-000000.gz'
for i in range(0,168):
    urllib.request.urlretrieve(url1[i], '/Users/williamkye/Box Sync/nyc data science academy/project_2/data/' + filename[i] + '.gz')
    print('completed:' + str(i))