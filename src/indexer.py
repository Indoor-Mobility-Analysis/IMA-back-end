from pymongo import MongoClient

HOST = '127.0.0.1'
PORT = 27017
DB = 'mapping'
INDEXFIELDS = {'people_count': ['day'],
               'people_activity': ['time_stamp'],
               'tickets_all': ['in_time', 'out_time'],
               'tickets_adm': ['time_stamp']}

class Indexer:
    def __init__(self, field=None):
        """
        Indexer to set index on specific fields in collections on MongoDB
        :param field: the fields in collections to create indexes on
        :return:
        """
        self.field = field if field is not None else INDEXFIELDS

    def create_indexes(self):
        """Create indexes for collections
        :return:
        """
        client = MongoClient(HOST, PORT)
        db = client[DB]
        collections = self.field.keys()
        for collection in collections:
            fields = self.field[collection]
            for field in fields:
                getattr(db, collection).create_index(field)
            print 'Indexes created on '+collection+':'
            print sorted(list(getattr(db, collection).index_information()))

if __name__ == '__main__':
    indexer = Indexer()
    indexer.create_indexes()

