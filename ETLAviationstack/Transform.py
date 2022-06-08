import pandas as pd
from datetime import datetime
import psycopg2
import mysql.connector

# transformar
data_transform_flights=flights_list
data_transform_airlines=airlines_list
data_transform_airports=airports_list

def transform_flights(list_data):
    date_inspection = datetime.now()
    df= pd.DataFrame();
    df=pd.DataFrame.from_dict(list_data);
    df['departure_scheduled']=pd.to_datetime(df['departure_scheduled'], format="%Y-%m-%d %H:%M:%S");
    df['departure_estimated']=pd.to_datetime(df['departure_estimated'], format="%Y-%m-%d %H:%M:%S");
    df['arrival_scheduled']=pd.to_datetime(df['arrival_scheduled'], format="%Y-%m-%d %H:%M:%S");
    df['arrival_estimated']=pd.to_datetime(df['arrival_estimated'], format="%Y-%m-%d %H:%M:%S");
    #del(df['departure_actual_runway']);
    del(df['aircraft']);
    del(df['live']);
    del(df['departure_delay']);
    del(df['departure_actual']);
    del(df['departure_estimated_runway']);
    del(df['departure_actual_runway']);
    del(df['arrival_gate']);
    df['date_transform']=date_inspection;
    return df;

def transform_airlines(list_data):
    date_inspection = datetime.now()
    df= pd.DataFrame();
    df=pd.DataFrame.from_dict(list_data);
    del(df['hub_code']);
    del(df['iata_code']);
    #del(df['iata_code']);
    df['date_transform']=date_inspection;
    return df


def transform_airports(list_data):
    date_inspection = datetime.now()
    df= pd.DataFrame();
    df=pd.DataFrame.from_dict(list_data);
    del(df['phone_number']);
    del(df['iata_code']);
    del(df['city_iata_code']);
    del(df['geoname_id']);
    df['date_transform']=date_inspection;
    return df

info_transform_flights=transform_flights(data_transform_flights);
info_transform_airlines=transform_airlines(data_transform_airlines)
data_transform_airports=transform_airports(data_transform_airports)
