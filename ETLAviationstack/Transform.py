import json
import boto3

#import pandas as pd
from datetime import datetime
s3 = boto3.client('s3')


def extraction_flights(flights_response, date_inspection):
    flights_list = []
    for element in flights_response:
        flight ={
            "flight_date":element['flight_date'],
            "flight_status":element['flight_status'],
            "departure_airport": element['departure']['airport'],
            "departure_timezone": element['departure']['timezone'],
            "departure_iata": element['departure']['iata'],
            "departure_icao": element['departure']['icao'],
            "departure_gate": element['departure']['gate'],
            "departure_delay": element['departure']['delay'],
            "departure_scheduled": element['departure']['scheduled'],
            "departure_estimated": element['departure']['estimated'],
            "departure_actual": element['departure']['actual'],
            "departure_estimated_runway": element['departure']['estimated_runway'],
            "departure_actual_runway": element['departure']['actual_runway'],
            "arrival_airport": element['arrival']['airport'],
            "arrival_timezone": element['arrival']['timezone'],
            "arrival_iata": element['arrival']['iata'],
            "arrival_icao": element['arrival']['icao'],
            "arrival_terminal": element['arrival']['terminal'],
            "arrival_gate": element['arrival']['gate'],
            "arrival_baggage": element['arrival']['baggage'],
            "arrival_delay": element['arrival']['delay'],
            "arrival_scheduled": element['arrival']['scheduled'],
            "arrival_estimated": element['arrival']['estimated'],
            "arrival_actual": element['arrival']['actual'],
            "arrival_estimated_runway": element['arrival']['estimated_runway'],
            "arrival_actual_runway": element['arrival']['actual_runway'],
            "airline_name": element['airline']['name'],
            "airline_iata": element['airline']['iata'],
            "airline_icao": element['airline']['icao'],
            "flight_number": element['flight']['number'],
            "flight_iata": element['flight']['iata'],
            "flight_icao": element['flight']['icao'],
            "flight_codeshared": element['flight']['codeshared'],
            "aircraft":"None",
            "live":"None",
            "date_inspection": date_inspection
            
        }
        flights_list.append(flight)
    return flights_list
    
def extraction_airports(airports_response, date_inspection):
    airports_list = []
    for element in airports_response:
        airport = {
            "airport_name": element['airport_name'],
            "iata_code": element['iata_code'],
            "icao_code": element['icao_code'],
            "latitude": element['latitude'],
            "longitude": element['longitude'],
            "geoname_id": element['geoname_id'],
            "timezone": element['timezone'],
            "gmt": element['gmt'],
            "phone_number": element['phone_number'],
            "country_name": element['country_name'],
            "country_iso2":element['country_iso2'],
            "city_iata_code":element['city_iata_code'],
            "date_inspection": date_inspection
        }
        airports_list.append(airport)
    return airports_list

def extraction_airlines(airlines_response, date_inspection):
    airlines_list = []
    for element in airlines_response:
        airline = {
            "airline_name": element['airline_name'],
            "iata_code": element['iata_code'],
            "iata_prefix_accounting": element['iata_prefix_accounting'],
            "icao_code": element['icao_code'],
            "callsign": element['callsign'],
            "type": element['type'],
            "status": element['status'],
            "fleet_size": element['fleet_size'],
            "fleet_average_age": element['fleet_average_age'],
            "date_founded": element['date_founded'],
            "hub_code": element['hub_code'],
            "country_name": element['country_name'],
            "country_iso2": element['country_iso2'],
            "date_inspection": date_inspection
        }
        airlines_list.append(airline)
    return airlines_list
    
def transform_flights(list_data):
    date_inspection = datetime.now()
    df= pd.DataFrame()
    df=pd.DataFrame.from_dict(list_data)
    df['departure_scheduled']=pd.to_datetime(df['departure_scheduled'], format="%Y-%m-%d %H:%M:%S")
    df['departure_estimated']=pd.to_datetime(df['departure_estimated'], format="%Y-%m-%d %H:%M:%S")
    df['arrival_scheduled']=pd.to_datetime(df['arrival_scheduled'], format="%Y-%m-%d %H:%M:%S")
    df['arrival_estimated']=pd.to_datetime(df['arrival_estimated'], format="%Y-%m-%d %H:%M:%S")
    #del(df['departure_actual_runway'])
    del(df['aircraft'])
    del(df['live'])
    del(df['departure_delay'])
    del(df['departure_actual'])
    del(df['departure_estimated_runway'])
    del(df['departure_actual_runway'])
    del(df['arrival_gate'])
    df['date_transform']=date_inspection
    return df

def transform_airlines(list_data):
    date_inspection = datetime.now()
    df= pd.DataFrame()
    df=pd.DataFrame.from_dict(list_data)
    del(df['hub_code'])
    del(df['iata_code'])
    #del(df['iata_code'])
    df['date_transform']=date_inspection
    return df


def transform_airports(list_data):
    date_inspection = datetime.now()
    df= pd.DataFrame()
    df=pd.DataFrame.from_dict(list_data)
    del(df['phone_number'])
    del(df['iata_code'])
    del(df['city_iata_code'])
    del(df['geoname_id'])
    df['date_transform']=date_inspection
    return df
#Event evento disparador de la lambda
for record in event['Records']:
        print(record)
        bucket = record['s3']['bucket']['name']
        archivo = record['s3']['object']['key']
        obj_archivo = s3.get_object(Bucket=bucket, Key=archivo)
        j = json.loads(obj_archivo['Body'].read())
        print(j)
data_transform_flights=extraction_flights(data)
data_transform_airlines=extraction_airlines(data)
data_transform_airports=extraction_airports(data)

#transformar
data_transform_flights= transform_flights(data_transform_flights)
data_transform_airlines= transform_airlines(data_transform_airlines)
data_transform_airports= transform_airports(data_transform_airports)