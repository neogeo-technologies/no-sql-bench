from bench_base import BenchBase
import psycopg2
import psycopg2.extras
import json

create_table_command = """
    CREATE TABLE {} (id char(64) primary key, name char(128), number int, small_number int, embedded text);
    """

create_index_command = """
    CREATE INDEX {}_small_number_idx ON {} (small_number);
    """

insert_template = """INSERT INTO {} VALUES (%s,%s,%s,%s,%s);"""

get_template = """
    SELECT * FROM {} WHERE id=%s;
    """

query_template = """
    SELECT * FROM {} WHERE {};
    """


def jsonify_text_fields(data):
    for key, value in data.items():
        if isinstance(value, basestring) and value[0] == "{":
            data[key] = json.loads(value)
    return data


class BenchPostgres(BenchBase):
    
    ID_FIELD = "id"

    def __init__(self, *args, **kwargs):
        self.host = "pg_server"
        self.port = 5432
        self.user = u"neogeo"
        self.password = u"myloosesuperpass"
        self.conn = None
        self.curs = None
        super(BenchPostgres, self).__init__(*args, **kwargs)

    def create_database(self):

        try:
            with psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database="postgres",
                    user=self.user,
                    password=self.password) as conn:
                conn.autocommit = True
                with conn.cursor() as curs:
                    curs.execute("CREATE DATABASE {}".format("test"))
        except:
            pass

        self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database="test",
                user=self.user,
                password=self.password)
        self.conn.autocommit = True
        self.curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        self.curs.execute(create_table_command.format(self.db_name))

    def delete_database(self):
        self.curs.close()
        self.conn.close()

        with psycopg2.connect(
                host=self.host,
                port=self.port,
                database="postgres",
                user=self.user,
                password=self.password) as conn:
            conn.autocommit = True
            with conn.cursor() as curs:
                curs.execute("DROP TABLE IF EXISTS {}".format(self.db_name))
            
    def create(self, record):
        for key, value in record.items():
            if isinstance(value, dict):
                record[key] = json.dumps(value)
        return self.curs.execute(insert_template.format(self.db_name), (record["id"], record["name"], record["number"], record["small_number"], record["embedded"],))

    def get(self, key):
        self.curs.execute(get_template.format(self.db_name), (key,))
        return jsonify_text_fields(self.curs.fetchone())

    def query(self, **kwargs):
        conditions = ["%s=%r" % item for item in kwargs.items()]
        query_statement = query_template.format(self.db_name, " and ".join(conditions))
        self.curs.execute(query_statement)
        results = self.curs.fetchall()
        return map(jsonify_text_fields, results)


class BenchPostgresIndexed(BenchPostgres):

    def create_database(self, *args, **kwargs):
        super(BenchPostgresIndexed, self).create_database(*args, **kwargs)
        self.curs.execute(create_index_command.format(self.db_name, self.db_name))
