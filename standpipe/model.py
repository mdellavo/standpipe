import bson


class Record(object):
    def __init__(self, stream_name, record=None, record_id=None):
        self.id = bson.ObjectId(record_id) or bson.ObjectId()
        self.stream_name = stream_name
        self.record = record

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return self.id != other.id

    def __lt__(self, other):
        return self.id < other.id

    def __le__(self, other):
        return self < other or self.id == other.id

    def __gt__(self, other):
        return self.id > other.id

    def __ge__(self, other):
        return self > other or self.id == other.id

    def __cmp__(self, other):
        return (other > self) - (other < self)

