import datetime
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.types import TIMESTAMP, NUMERIC

import requests as req
import pandas as pd



URL = 'https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getWindForecast&returnType=json'


def get_forecast_data(url=URL):
    """
    Takes URL (url) and fetches weather forecast data from misoenergy.org.
    """
    response = req.get(url)
    json_data = response.json()
    data = json_data['Forecast']

    df = pd.io.json.json_normalize(data[0])
    for data in data[1:]:
        df = df.append(pd.io.json.json_normalize(data))
    
    return df

# I do not need to clean data in my forecast coding, therefore I commented out this section.

"""

def clean_df(df):
    """
    # Cleans dataframe up by converting datetimes datatypes and dropping
    # unnecessary columns.
    """
    # clean up df datatypes
    # Z means Zulu time, or GMT or UTC
    df['created'] = pd.to_datetime(df['created'], utc=True)
    df['applicable_date'] = pd.to_datetime(df['applicable_date']).dt.tz_localize('US/Mountain')

    # drop unneccesary columns
    df.drop(['weather_state_abbr', 'id'], inplace=True, axis=1)
    return df
    
"""

def store_data(df):
    """
    Takes dataframe (df) and stores it in MongoDB.
    """
    conn = psycopg2.connect("dbname=miso host=localhost user=postgres password=postgres")
    cursor = conn.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS wind_forecast (datetime TIMESTAMP WITH TIME ZONE, value NUMERIC);')
    conn.commit()  # required to actually execute statements
    conn.close()

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/miso')

    df.columns = [d.lower() for d in df.columns]
    df['datetime'] = df['datetimeest']

    df[['datetime', 'value']].to_sql(name='wind_forecast',
                                     con=engine,
                                     if_exists='append',
                                     index=False,
                                     dtype={'datetime': TIMESTAMP(timezone=True),
                                            'value': NUMERIC})
    engine.dispose()


def daily_scrape():
    """
    Scrapes data once per day at specified time.
    
    A better way to do this might be to check once per minute (or hour), 
    and only scrape if the utc_time.hour hits a specified hour and it hasn't scraped
    yet that day.  A boolean variable could be used as a flag as to whether or 
    not it has scraped that day, or a variable which stores the last scraped day
    from utc_time.day.
    
    Another way is to use crontab.  Running 'crontab -e' from the console allows 
    editing of the crontab file, and files can be set to run there periodically.
    """

    last_scrape = None
    while True:
        utc_time = datetime.datetime.utcnow()
        if utc_time.date() != last_scrape:
            df = get_forecast_data()
            # df = clean_df(df)     since I commented out the clean_df ...
            store_data(df)
            last_scrape = utc_time.date()
            time.sleep(1)  # sleep one second to ensure we don't double-scrape
        

    
if __name__ == "__main__":
    df = get_forecast_data()
    # df = clean_df(df)     since I commented out the clean_df ...
    store_data(df)