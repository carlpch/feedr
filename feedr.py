import os
import psycopg2 as psycopg
import configparser
import requests
import json
import time

config = configparser.ConfigParser()
config.read('config.ini')
# print(config.sections()) # ['postgres']

connection = psycopg.connect(
	host = config['postgres']['host'],
	database = config['postgres']['database'],
	user = config['postgres']['user'],
	password = config['postgres']['password']
	)

cursor = connection.cursor()
cursor.execute('SELECT version()')

code = open('feeder.sql','r').read()
cursor.execute(code)
connection.commit() # thought it was cursor.commit()...

def weather_api_connect(max_attempt):
	url = 'https://api.weather.gov/stations/KCHO/observations?limit=10'
	attempt = 0
	while attempt < max_attempt:
		response = requests.get(url)
		if response.status_code == 503:
			print('503 Error--service not available')
			time.sleep(5)
			attempt += 1
		else:
			print('Status code:', response.status_code)
			print('json data saved to `j`')
			j = response.json()
			break
	return j

j = weather_api_connect(5)

latest_weather = j['features'][0]['properties']
keys = list(latest_weather.keys())
# print(list(keys[4:]))
# print([type(j['features'][0]['properties'][key]) for key in keys[4:]])

text_properties = ['timestamp', 'rawMessage', 'textDescription'] # no need for 'icon'
dict_properties = ['temperature', 'dewpoint', 'windDirection', 
					'windSpeed', 'windGust', 'barometricPressure', 'seaLevelPressure', 
					'visibility', 'maxTemperatureLast24Hours', 'minTemperatureLast24Hours', 
					'precipitationLastHour', 'precipitationLast3Hours', 'precipitationLast6Hours', 
					'relativeHumidity', 'windChill', 'heatIndex']

output = []

for i in text_properties:
	output.append(latest_weather[i])

for j in dict_properties:
	output.append(latest_weather[j].get('value'))

columns = ','.join(text_properties+dict_properties)
values = str(tuple(output)).replace('None','NULL')

insertion = 'INSERT INTO weather ({}) VALUES {} ON CONFLICT DO NOTHING'.format(columns, values)
try:
	cursor.execute(insertion)
except:
	print('Error. Key already exists?')
connection.commit()


