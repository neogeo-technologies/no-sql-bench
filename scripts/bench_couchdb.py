from bench_base import BenchBase
from pycouchdb import *

query_template = """
    function(doc) {
        if (%s) {
            emit(doc);
        }
    }
    """

class BenchCouchDB(BenchBase):

    ID_FIELD = "_id"

    def __init__(self, *args, **kwargs):
        self.connexion_uri = 'http://{0}:{1}@{2}'.format(u"neogeo", u"myloosesuperpass", "couchdb_server:5984")
        self.server = Server(self.connexion_uri)
        super(BenchCouchDB, self).__init__(*args, **kwargs)

    def create_database(self):
        self.db = self.server.create(self.db_name)

    def delete_database(self):
        self.server.delete(self.db_name)

    def create(self, record):
        self.db.save(record)

    def get(self, key):
        return self.db.get(key)

    def query(self, **kwargs):
        conditions = ["doc.%s == %r" % item for item in kwargs.items()]
        query_fn = query_template % " && ".join(conditions)
        results = self.db.temporary_query(query_fn)
        count = len(list(results)) # force a lookup, in case it's lazy
        return results
