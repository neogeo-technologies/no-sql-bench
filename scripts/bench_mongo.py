from bench_base import BenchBase
from pymongo import MongoClient
from pymongo import ASCENDING


class BenchMongoDB(BenchBase):
    
    ID_FIELD = "_id"

    def __init__(self, *args, **kwargs):
        self.connexion_uri = 'mongodb://{0}:{1}@{2}'.format(u"neogeo", u"myloosesuperpass", "mongodb_server:27017")
        self.client = MongoClient(self.connexion_uri)
        super(BenchMongoDB, self).__init__(*args, **kwargs)
    
    def create_database(self):
        self.db = self.client[self.db_name]
        self.collection = self.db.test_collection

    def delete_database(self):
        self.client.drop_database(self.db_name)
            
    def create(self, record):
        self.collection.insert(record)
    
    def get(self, key):
        return self.collection.find_one({"_id": key})
        
    def query(self, **kwargs):
        return list(self.collection.find(kwargs))
    

class BenchMongoDBIndexed(BenchMongoDB):

    def create_database(self, *args, **kwargs):
        super(BenchMongoDBIndexed, self).create_database(*args, **kwargs)
        self.collection.create_index([("small_number", ASCENDING)])
