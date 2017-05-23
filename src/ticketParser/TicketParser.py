import datetime as dt
HOST = '127.0.0.1'
PORT = 27017
DB = 'mapping'
ALLCOLLECTION = 'tickets_all'
SCHEMA = ['in_station', 'out_station', 'in_time', 'out_time', 'gate']
SCHEMATYPE = ['str', 'str', 'time', 'time', 'int']
STARTTIME = "01/01/1970 06:30:00"
ENDTIME = "01/01/1970 08:00:00"
STATIONS = ['ADM']
ALLSTATION = True


class TicketParser:
    def __init__(self, path, schema=None, all=False):
        """
        Ticket data path(csv file, with schema: SCHEMA)
        :param path: path of data path, if no path set, fetch data from 'path'
        :return:
        """
        self.schema = schema if schema != None else SCHEMA
        self.ticket_path = path
        self.record_container = self.__record_reader_generator()
        self.start_time = self.parse_time(STARTTIME)
        self.end_time = self.parse_time(ENDTIME)
        self.store_all = all

    def __record_reader_generator(self):
        """
        Record generator, no explaination
        :return:
        """
        with open(self.ticket_path, 'r') as input:
            line = input.readline()
            while line:
                segs = line.split(',')
                segs = [seg.strip() for seg in segs]
                format_elements = self.__format_record(segs)
                yield(format_elements)
                line = input.readline()
        while True:
            yield None

    def __format_element(self, e, type):
        """
        Convert data to specific type
        :param e: the element to be converted, must be string
        :param type: output type
        :return: output with type
        """
        if type == 'str':
            return str(e)
        if type == 'int':
            return int(e)
        if type == 'time':
            return str(e)

    def __format_record(self, segs):
        """
        Format an element
        :param segs: the elements of each record
        :return: formated elments
        """
        output_obj = {}
        for index in range(0, len(segs)):
            schema, e, type = self.schema[index], segs[index], SCHEMATYPE[index]
            output_obj[schema] = self.__format_element(e, type)
        return output_obj

    def parse_time(self, str_time):
        return dt.datetime.strptime(str_time, '%d/%m/%Y %H:%M:%S')

    def read_record(self):
        """
        Read current record and move the iterator to the next one,
        return None if iterator point to the one next to last one
        :return: one record
        """
        return self.record_container.next()

    def map_time(self, str_time):
        """
        Map time to [self.start_time, self.end_time]
        :param str_time: time
        :return: transformed time
        """
        return int((self.parse_time(str_time) - self.start_time).total_seconds())

    def distribute_records(self):
        """
        Insert the record into the mongodb, only station defined in STATIONS are considered
        :return:
        """
        from pymongo import MongoClient
        client = MongoClient(HOST, PORT)
        db = client[DB]
        collection_all = db[ALLCOLLECTION]
        collection_all.remove({})
        # Init collection
        collections = {}
        for station in STATIONS:
            collection_name = 'tickets_' + station
            collections[collection_name] = db[collection_name]
            collections[collection_name].remove({})

        record = self.read_record()
        number = 0
        while record:
            if number % 100 == 0:
                print(number, 'of records has been parsed!')
            number += 1

            if self.record_in_range(record) == False:
                record = self.read_record()
                continue

            if self.store_all == True:
                collection_all.insert(record)

            in_station, out_station = record['in_station'], record['out_station']
            in_time, out_time = record['in_time'], record['out_time']
            if in_station in STATIONS:
                if self.record_in_range(record, in_station):
                    ins_obj = {
                        'gate': record['gate'],
                        'io': True,
                        'timestamp': self.map_time(in_time),
                        'station': in_station,
                        'n': number
                    }
                    collections['tickets_' + in_station].insert(ins_obj)

            if out_station in STATIONS:
                if self.record_in_range(record, out_station):
                    ins_obj = {
                        'gate': record['gate'],
                        'io': False,
                        'timestamp': self.map_time(out_time),
                        'station': out_station,
                        'n': number
                    }
                    collections['tickets_' + out_station].insert(ins_obj)

            record = self.read_record()


    def record_in_range(self, record, station=None):
        """
        If a record appears in the range[self.start_time, self.end_time]
        :param record: a record which will be judge
        :param station: specific station
        :return: True in the range, false not
        """
        in_time = self.parse_time(record['in_time'])
        out_time = self.parse_time(record['out_time'])

        if station == None:
            if in_time >= self.start_time and in_time <= self.end_time:
                return True
            if out_time >= self.start_time and out_time <= self.end_time:
                return True
            return False
        else:
            if station not in STATIONS:
                return False
            if record['in_station'] == station:
                if in_time >= self.start_time and in_time <= self.end_time:
                    return True
            if record['out_station'] == station:
                if out_time >= self.start_time and out_time <= self.end_time:
                    return True
            return False


if __name__ == '__main__':
    parser = TicketParser('../data/ticket.csv', all=True)
    parser.distribute_records()