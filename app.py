import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd
import datetime

app = Flask(__name__) 

############# FUNCTIONS #############
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

# Station Functions
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'


# Trip Functions
def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'
######################################


############# ENDPOINTS #############
@app.route('/')
@app.route('/homepage')
def home():
    hw = 'Hello' + '\n' + 'World'
    return hw

#---- Stations ----#
@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result
#-------------------#

#---- Trips ----#
@app.route('/trips/')
def route_all_trips():
    return get_all_trips(make_connection()).to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    return get_trip_id(trip_id, make_connection()).to_json()

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result
#---------------#

@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

### Create Static Endpoint(s)
@app.route('/trips/most-used-routes')
def routes_modus():
    journey = pd.read_sql_query("""
    SELECT start_Station_name||',,,'||end_Station_name AS start_to_end, duration_minutes
    FROM trips
    """,make_connection())
    journey['start_to_end']=journey['start_to_end'].astype('category')# Fetching data dan ubah tipe data

    modus = pd.crosstab(journey['start_to_end'],'count')\
    .sort_values(by='count',ascending=False).head(5).reset_index()# mencari tau rute terbanyak

    modusDuration = journey[journey['start_to_end'].isin(modus['start_to_end'])]
    modusDuration = pd.crosstab(modusDuration['start_to_end'],'mean',
               values=modusDuration['duration_minutes'],
                aggfunc='mean').round(0)# mencari tau rata2 durasi perjalanan dari modus rute

    hasil = modus.copy()

    temporary=[]
    for i in hasil['start_to_end']:
        temporary.append(modusDuration.loc[i]['mean'])
    hasil['Average_Duration_minutes'] = temporary# menggabungkan modus dan modusDuration

    hasil[['start_station_name','end_station_name']]=\
    hasil['start_to_end'].str.split(',,,',1, expand=True)
    hasil=hasil.drop(columns='start_to_end')
    hasil = hasil[[ 'start_station_name','end_station_name','count', 'Average_Duration_minutes']]# merapikan hasil akhir
    return hasil.to_json()

### Create Dynamic Endpoints
@app.route('/trips/bike-stats/<bike_id>')
def bike_statistics(bike_id):
    bike=pd.read_sql_query(f"""
    SELECT bikeid, start_station_name, end_Station_name, duration_minutes
    FROM trips
    WHERE bikeid = {bike_id}
    """, make_connection())# fetching data
    
    if bike.shape[0] == 0:
        return "bike id not found"# jika bike_id tidak ada di database
    
    bikeStationHistory=bike['start_station_name'].copy()+bike['end_station_name'].copy()# menggabungkan start dan end station ke dalam satu list
    bikeStationHistory=list(dict.fromkeys(bikeStationHistory))# menghapus duplikat
    bikeStationHistory=len(bikeStationHistory)# menghitung jumlah stasiun

    bikeTotalDuration = float(bike['duration_minutes'].sum())# menghitung total duration
    bikeTotalDuration = str(datetime.timedelta(minutes=bikeTotalDuration))# convert data menit jadi datetime

    hasil = f'bike id {bike_id} has been to {bikeStationHistory} different stations and its total travel time is {bikeTotalDuration}'
    return hasil

### Create Post Endpoints
@app.route('/trips/bike-id-station/json', methods=['POST'])
def bike_station():
    req = request.get_json(force=True)
    station_id=req['station']
    stationStat = pd.read_sql_query(
    f"""
    SELECT bikeid
    FROM trips
    WHERE start_station_id = {station_id} OR end_station_id = {station_id}
    """,make_connection())
    if stationStat.shape[0]==0:
        return 'station id not found'
    hasil=pd.crosstab(stationStat['bikeid'],'count').sort_values(by='count',ascending=False).head(10)
    return hasil.to_json()

######################################

if __name__ == '__main__':
    app.run(debug=True, port=5000)