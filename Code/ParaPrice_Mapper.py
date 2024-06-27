# -*- coding: utf-8 -*-
"""
Created on Tue Apr  6 14:28:17 2021

@author: rchokkam
"""
import pandas as pd
import numpy as np
import time
import math

df = pd.read_csv('../DataSets/Extracted_Para_Apple.csv')
# df['DateTime'] = df.Date + "T" + df.Time
print (df.head())
# df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
# df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
# df['Date'] = df['Date'].dt.strftime('%Y.%m.%d')
# df['Time'] = pd.to_datetime(df['Time'])
# df['Time'] = df['Time'].dt.strftime('%H:%M')
# df['DateTime'] = df['Date'] + ' ' + df['Time']

# # Convert to datetime object
# df['DateTime'] = pd.to_datetime(df['DateTime'], dayfirst=True, format='mixed')

# # Format datetime object to desired string format
# df['DateTime'] = df['DateTime'].dt.strftime('%Y.%m.%d %H:%M')
# df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y.%m.%d %H:%M', utc=True)
df['DateTime'] = pd.to_datetime(df.DateTime,utc=True)
print (df.head())

interval = 60
stock = 'Apple'
news_time = 60

csv_name = '../DataSets/Extract/NewsGroupTime_60/' + str(stock) + str(interval) + '.csv'

charts_path = '../../PreprocessedData-20240223/Charts/amazon60_preprocessed.csv'
# charts_path += str.upper(stock) + str(interval) + '.csv'

chart_data = pd.read_csv(charts_path)

# print(chart_data.head())

chart_data['DateTime'] = chart_data['Date'] + ' ' + chart_data['Time']
chart_data['DateTime'] = pd.to_datetime(chart_data['DateTime'], dayfirst=True)
chart_data['DateTime'] = chart_data['DateTime'].dt.strftime('%Y.%m.%d %H:%M')
chart_data['Movement'] = np.where(chart_data['Open'] < chart_data['Close'],1,0)
chart_data['DateTime'] = pd.to_datetime(chart_data['DateTime'], format='%Y.%m.%d %H:%M', utc=True)

print (chart_data.head())
mt = time.time()
print("Mapping begins")

ch_arts = 0
pct = 0
data = pd.DataFrame(columns=['Source', 'News', 'Movement'])
for news_index, news in df.iterrows():
    ch_arts += 1
    if (math.floor(ch_arts*100/df.shape[0]) - pct) > 0:
        pct = math.floor(ch_arts*100/df.shape[0])
        print("Articles checked =", pct, "%")
    # if news[stock] == 'Yes':
    for chart_index, chart in chart_data.iterrows():
        react_time = (chart.DateTime - news.DateTime).total_seconds()
        if react_time >= news_time*60 and react_time < (interval+news_time)*60:
            new_link = {'Source': news.Source, 'News': news.text, 'Movement': chart.Movement, 'Date': chart.Date}
            data = data._append(new_link, ignore_index = True)
            if data.shape[0] % 100 == 0:
                print(data.shape[0], "news articles mapped")
            break
        elif react_time < 0:
            chart_data.drop(chart_index, inplace = True)
        if react_time > (interval+news_time)*60:
            break


data.to_csv(csv_name, index = False)
et = time.time() - mt
print("Mapping time is", int((et) // (60*60)), "hours", int(((et) % (60*60)) // 60), "minutes", int((et) % 60), "seconds")
print("Mapped", stock, "with", interval, "minutes interval")
# df.to_csv('../DataSets/Extract/amazon_sentiment_movement' + str(interval) + '.csv', index=False) 