# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 12:46:35 2021

@author: rchokkam
"""
import pandas as pd
import os
import time
import json

stock = 'Amazon'
if stock == 'Apple':
    sym = 'AAPL'
elif stock == 'Amazon':
    sym = 'AMZN'
    
st = time.time()
df = pd.DataFrame(columns=['Source', 'text', 'DateTime'])
new_rec = pd.DataFrame()
count = 0
fpath = '../../News/'
for root, subdirs, files in os.walk(fpath, topdown=True):
    # print(root)
    # print(subdirs)
    # print(files)
    for file in files:
        # print(file)
        if 'json' in file:
            count += 1
            if count % 100 == 0:
                print("Articles read =", count)
            json_path = os.path.join(root, file)
            # print("Json_path = " + json_path)
            with open(json_path, 'r', encoding='utf-8') as file:
                # for line in file: print(line)
                data_list = json.load(file)
                json_data = pd.json_normalize(data_list)
                # print(json_data.head())
                if json_data.language[0] != 'english' or len(json_data.text[0]) > 32767:
                    continue
                new_rec['Source'] = json_data['thread.site_full']
                new_rec[stock] = 'No'
                new_rec['text'] = ''
                new_rec['DateTime'] = pd.to_datetime(json_data.published,utc=True)
                # print(new_rec)
                temp = ""
                for para in json_data.text[0].split('\n'):
                    if sym in para or stock in para:
                        temp += para
                        new_rec[stock] = 'Yes'
                        # print(new_rec['text'])
                new_rec['text'] = temp
                # print("TEXT : " + new_rec['text'])
                if new_rec[stock][0] == 'Yes':
                    new_rec.drop(columns=[stock], inplace=True)
                    df = df._append(new_rec, ignore_index=True)
                    # print("Columns : " +  df.columns)
            # break
        

    

# print(df.head())

df = df.sort_values(by='DateTime')
csv_name = '../DataSets/Extracted_Para_' + str(stock) + '.csv'
df.to_csv(csv_name, index = False)

mt = time.time()
et = st - mt
print("Reading time is", int((et) // (60*60)), "hours", int(((et) % (60*60)) // 60), "minutes", int((et) % 60), "seconds")