"""
This module parses the ticketing data in .txt format, 
truncates it by a time period.

It provides two functions, one to write the
dealt data into json files, and the other to insert the
data directly into MongoDB.
"""

import urllib2
import datetime as dt
import json
from pymongo import MongoClient
import pymongo
HOST = '127.0.0.1'
PORT = 27017
DB = 'mapping'
client = MongoClient(HOST, PORT)
db = client[DB]

path = 'http://maxwell.ielm.ust.hk/thales/final.txt'
ticketing_data = urllib2.urlopen(path).read().split('\n')

init_time = None
end_time = dt.datetime.strptime('01/01/1900 08:00:00', '%d/%m/%Y %H:%M:%S')

ticketing_adm = []
ticketing_all = []
i = 0
for line in ticketing_data:
    adm_entry = {}
    all_entry = {}
    if len(line) > 2:
        line = line.split(',')
        if i == 0:
            station_name = line[0]
            if init_time is None:
                init_time = dt.datetime.strptime(line[2], '%d/%m/%Y %H:%M:%S')
            end_time = int((end_time - init_time).total_seconds())
            i = 1
        in_station = line[0]
        out_station = line[1]
        in_time = dt.datetime.strptime(line[2], '%d/%m/%Y %H:%M:%S')
        in_time = int((in_time - init_time).total_seconds())
        if in_time > end_time:
            break
        out_time = dt.datetime.strptime(line[3], '%d/%m/%Y %H:%M:%S')
        out_time = int((out_time - init_time).total_seconds())
        gate = int(line[4])

        all_entry['in_station'] = in_station
        all_entry['out_station'] = out_station
        all_entry['in_time'] = in_time
        all_entry['out_time'] = out_time
        
        adm_entry['gate'] = gate
        
        if line[0] == station_name:
            adm_entry['io'] = True
            adm_entry['timestamp'] = in_time
            all_entry['in_gate'] = gate
            all_entry['out_gate'] = 'null'
        else:
            adm_entry['io'] = False
            adm_entry['timestamp'] = out_time
            all_entry['in_gate'] = 'null'
            all_entry['out_time'] = gate
            
        ticketing_adm.append(adm_entry)
        ticketing_all.append(all_entry)
        
def output_json():
    with open('ticketing_adm.json', 'w') as f:
        json.dump(ticketing_adm, f)
    with open('ticketing_all.json', 'w') as f:
        json.dump(ticketing_all, f)
        
def insert_into_db():
    result1 = db.ticketing_adm.insert_many(ticketing_adm)
    result2 = db.ticketing_all.insert_many(ticketing_all)
    return result1, result2