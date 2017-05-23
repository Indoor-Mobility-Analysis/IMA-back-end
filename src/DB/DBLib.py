from pymongo import MongoClient
HOST = "127.0.0.1"
PORT = 27017
DB = "mapping"

collection_names = ["people_activity", "people_count", "tickets_all", "tickets_adm"]


def create_index(c_name=None, attr=None, order=1):
    client = MongoClient(HOST, PORT)
    db = client[DB]
    if c_name == None or attr == None:
        return
    if c_name == "all":
        for c_name in collection_names:
            collection = db[c_name]
            collection.create_index(attr)
    else:
        collection = db[c_name]
        collection.create_index(attr)

if __name__ == '__main__':
    create_index('all', 'time_stamp', 1)