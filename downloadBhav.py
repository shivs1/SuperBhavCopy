# Copyright (c) 2016 Vashistha Iyer
# Twitter.com/Uptickr

import requests, pandas as pd, numpy as np
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime


def getBhav (date):

    date = datetime.strptime(date, '%Y%m%d')

    headers = { 'Accept' : '*/*',
                'User-Agent' : 'Mozilla/5.0',
                'Referer' : 'http://www.nseindia.com',
                'Connection' : 'keep-alive'
                }

    url_BhavCopy = 'http://www.nseindia.com/content/historical/EQUITIES/%s/%s/cm%s%s%sbhav.csv' \
                   '.zip' % \
                   (date.strftime('%Y'),
                    date.strftime('%b').upper(),
                    date.strftime('%d'),
                    date.strftime('%b').upper(),
                    date.strftime('%Y'))

    url_Delivery = 'http://www.nseindia.com/archives/equities/mto/MTO_%s%s%s.DAT' % \
                   (date.strftime('%d'),
                    date.strftime('%m'),
                    date.strftime('%Y'))

    ## Get BhavCopy and store only EQ & BE Series

    getLink = requests.get(url_BhavCopy, headers=headers)
    getContent = ZipFile(BytesIO(getLink.content))
    csvFile = getContent.open(getContent.namelist()[0])

    df1 = pd.read_csv(csvFile, index_col = 0).dropna(axis=1,how='all')
    df1 = df1[df1.apply(lambda x: x['SERIES']=='EQ' or x['SERIES']=='BE' , axis=1)]
    df1.drop(['LAST', 'PREVCLOSE'], axis=1, inplace=True)

    ## Get Delivery Volumes

    getLink = requests.get(url_Delivery, headers=headers)
    csvFile = BytesIO(getLink.content)
    df2 = pd.read_csv(csvFile, sep=',', header=None, skiprows=4, index_col = 2)
    df2.drop([0,1,4,6], axis=1, inplace=True)
    df2.index.name = 'SYMBOL'
    df2.columns = ['SERIES','TOTDLYQTY']
    df2 = df2[df2.apply(lambda x: x['SERIES']=='EQ' or x['SERIES']=='BE' , axis=1)]

    ## Merge and clean up to get final Dataset

    df = pd.merge(df1, df2, how='left', left_index=True, right_index=True)
    df.drop('SERIES_y', axis=1, inplace=True)
    df.rename(columns={'SERIES_x': 'SERIES'}, inplace=True)
    df['SERIES'].fillna('BE')
    df['TOTDLYQTY'] = np.where(df['TOTDLYQTY'].isnull()==True, df['TOTTRDQTY'], df['TOTDLYQTY'])
    df.to_csv(getContent.namelist()[0])

getBhav('20160226')
