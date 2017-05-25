from pymongo import MongoClient

client = MongoClient("127.0.0.1", 27017)
db = client['mapping']
collection = db['tickets_ADM']
m = {}
n = 0
for record in collection.find():
    n += 1
    IO = "i" if record['io'] == True else "o"
    gate = record['gate']
    k = IO + '_' + str(gate)
    if k not in m:
        m[k] = 0
    m[k] += 1

print(m)