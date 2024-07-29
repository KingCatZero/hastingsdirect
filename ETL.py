from os import path, getcwd, remove
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
pd.options.mode.chained_assignment = None
import boto3
from botocore.client import Config
import gzip

def getCurrentMonth():
    return datetime.today().strftime('%Y-%m')

def getMonthRange(startMonth, endMonth):
    currentDt = datetime(int(startMonth[:4]), int(startMonth[5:7]), 1)
    endDt = datetime(int(endMonth[:4]), int(endMonth[5:7]), 1)   

    while currentDt <= endDt:
        pad = '0' if currentDt.month < 10 else ''
        yield f'{currentDt.year}-{pad}{currentDt.month}'
        currentDt += relativedelta(months=1)

def getS3Creds():
    filepath = path.join(getcwd(), 's3Creds.txt')

    with open(filepath, 'r') as file:
        keys = file.readline().split(' ')

    return keys

def lazyReadPostcodes():
    filepath = path.join(getcwd(), 'ukpostcodessample.csv')

    with open(filepath, 'r') as file:
        next(file)

        for line in file:
            cols = line.replace('\n', '').split(',')
            yield [cols[1].replace(' ', ''), float(cols[2]), float(cols[3])]

def buildUrl(lat, long, month):
    return f'https://data.police.uk/api/crimes-street/all-crime?lat={lat}&lng={long}&date={month}'

def buildFilepathOut(postcode, month):
    return path.join(getcwd(), 'daily-dumps', f'{postcode}-{month.replace('-', '')}.csv')

def extract(lat, long, month):
    try:
        return pd.json_normalize(requests.get(buildUrl(lat, long, month)).json())
    except:
        return pd.DataFrame()

def transform(df, lat, long, postcode, month):
    df = df[[
        'category',
        'location_type',
        'context',
        'persistent_id',
        'id',
        'location_subtype',
        'month',
        'location.latitude',
        'location.street.id',
        'location.street.name',
        'location.longitude'
        ]]
    df['location.street.name'] = df['location.street.name'].apply(lambda x: x.replace('On or near ', ''))
    df['report_latitude'] = lat
    df['report_longitude'] = long
    df['report_postcode'] = postcode
    df['report_month'] = month
    return df

def s3Client(accessKey, secretAccessKey):
    return boto3.client('s3', aws_access_key_id=accessKey, aws_secret_access_key=secretAccessKey)

def s3Upload(filepath, client):
    key = f'daily-dumps/{path.basename(filepath)}'
    client.upload_file(filepath, 'hastingsdirect-streetcrime', key)

def load(df, filepath, client):
    df.to_csv(filepath, index=False)
    filepathGz = f'{filepath}.gz'

    with open(filepath, 'rb') as src, gzip.open(filepathGz, 'wb') as dst:
        dst.writelines(src)

    s3Upload(filepathGz, client)
    remove(filepath)
    remove(filepathGz)

def main(startMonth, endMonth):
    startMonth = startMonth if startMonth != '' else '2023-01'
    endMonth = endMonth if endMonth != '' else getCurrentMonth()
    s3AccessKey, s3SecretAccessKey = getS3Creds()
    client = s3Client(s3AccessKey, s3SecretAccessKey)

    for postcode, lat, long in lazyReadPostcodes():
        for month in getMonthRange(startMonth, endMonth):
            print('loading for', postcode, month)
            df = extract(lat, long, month)
            
            if not df.empty:
                df = transform(df, lat, long, postcode, month)
                load(df, buildFilepathOut(postcode, month), client)

if __name__ == '__main__':
    startMonth = '2024-01'
    endMonth = '2024-04'
    main(startMonth, endMonth)