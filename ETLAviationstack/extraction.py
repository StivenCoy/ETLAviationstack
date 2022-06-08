import requests
from datetime import datetime
import os

params = {
    'access_key': '' # os.getenv("acces_key")
}
URL_BASE = 'http://api.aviationstack.com/v1/'

endpoints = {
    "AIRPORTS": 'airports',
    "FLIGHTS": 'flights',
    "AIRLINES": 'airlines',
    "AIRPLANES": 'airplanes',
    "AIRCRAFT_TYPES": 'aircraft_types',
    "TAXES": 'taxes',
    "CITIES": 'cities',
    "COUNTRIES": 'countries'
}

#Llamada a la API
api_result_airlines = requests.get(URL_BASE+endpoints['AIRLINES'], params)
api_result_flights = requests.get(URL_BASE+endpoints['FLIGHTS'], params)
api_result_airports = requests.get(URL_BASE+endpoints['AIRPORTS'], params)


# Carga en formato Json
api_response_airlines = api_result_airlines.json()
api_response_flights = api_result_flights.json()
api_response_airports = api_result_airports.json()

if (api_result_airlines.ok and api_result_flights.ok and api_result_airports.ok):
    print('Respuesta Exitosa')
    #Aplica local
    file = open("./ETLAviationstack/flights.json", "w")
    json.dump(api_response_flights, file)
    file.close()
    file = open("./ETLAviationstack/airlines.json", "w")
    json.dump(api_response_airlines, file)
    file.close()
    file = open("./ETLAviationstack/airports.json", "w")
    json.dump(api_response_airports, file)
    file.close()
    #Todo implementar almacenamiento en s3 de los archivos
else:
    print('Capture un error en la obtencion de la data')