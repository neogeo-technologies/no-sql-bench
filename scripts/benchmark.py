#  -*- coding: utf-8 -*-

import bench_couchdb
import bench_mongo
import bench_postgres
import bench_postgres_jsonb
# import bench_sqlite
# import bench_jsonstore
# import bench_mysql
# import bench_codernity
# import bench_offtheshelf
# import bench_nosqlite

import pprint

from math import sqrt
import time

dbs = {
    # "sqlite3": bench_sqlite.BenchSQLite,
    # "sqlite3_ind": bench_sqlite.BenchSQLiteIndexed,
    # "jsonstore": bench_jsonstore.BenchJsonStore,
    # "mysql": bench_mysql.BenchMySQL,
    # "mysql_ind": bench_mysql.BenchMySQLIndexed,
    # "codernity_ind": bench_codernity.BenchCodernityDB,
    # "offtheshelf": bench_offtheshelf.BenchOffTheShelf,
    # "nosqlite": bench_nosqlite.BenchNoSQLite,
    "couchdb": bench_couchdb.BenchCouchDB,
    "mongodb": bench_mongo.BenchMongoDB,
    "mongodb_ind": bench_mongo.BenchMongoDBIndexed,
    "postgresql": bench_postgres.BenchPostgres,
    "postgresql_ind": bench_postgres.BenchPostgresIndexed,
    "postgresql_jsonb": bench_postgres_jsonb.BenchPostgresJsonb,
    "postgresql_jsonb_ind": bench_postgres_jsonb.BenchPostgresJsonbIndexed
}


def benchmark_function(cls, function, count, iterations, **kwargs):
    times = []

    bench = None

    if function != "create_records":
        bench = cls(count=count)
        bench.create_records()

    time.sleep(5)

    for i in range(iterations):
        if function == "create_records":
            bench = cls(count=count)
        start = time.time()
        getattr(bench, function)(**kwargs)
        stop = time.time()
        times.append(stop - start)

    return times
    

def compare_function(function, count, iterations, **kwargs):
    results = {}
    for db_name, db in dbs.items():
        print("\t{}".format(db_name))

        times = benchmark_function(db, function, count, iterations, **kwargs)
        n = float(len(times))
        mean = sum(times) / n
        std = sqrt(sum((x-mean)**2 for x in times) / n)
        result = results[db_name] = {
            "mean": mean,
            "std": std,
        }

        result["count"] = count
        result["iterations"] = iterations

        first = times[0]
        mean_after_first = None
        if n > 1:
            mean_after_first = sum(times[1:]) / (n-1)
        
        # if the first run was particularly different, log it separately
        if mean_after_first and abs(first - mean_after_first) > std * 2 :
            result["mean_after_first"] = mean_after_first
            result["first"] = first
            
    min_mean = min([result["mean"] for result in results.values()])
    for result in results.values():
        result["factor"] = result["mean"] / float(min_mean)
        if "mean_after_first" in result:
            result["factor_after_first"] = result["mean_after_first"] / float(min_mean)
            result["factor_first"] = result["first"] / float(min_mean)
    return results


def create_records_command():

    results = {}

    params = ((100,5), (1000,5), (10000, 5), (100000, 5), (1000000,5))
    nb_iters = len(params)

    for i in range(nb_iters):
          results["create_records {0}".format(i)] = compare_function(
              "create_records", count=params[i][0], iterations=params[i][1])

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(results)


def lookup_by_id_command():

    results = {}

    params = ((100,5), (1000,5), (10000, 5), (100000, 5), (1000000,5))
    nb_iters = len(params)

    for i in range(nb_iters):
          results["lookup_by_id {0}".format(i)] = compare_function(
              "get_random_specific_record", count=params[i][0], iterations=params[i][1])

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(results)


def query_command():

    results = {}

    params = ((100,5), (1000,5), (10000, 5), (100000, 5), (1000000,5))
    nb_iters = len(params)

    for i in range(nb_iters):
          results["query {0}".format(i)] = compare_function(
              "query", count=params[i][0], iterations=params[i][1], small_number=i)

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(results)


if __name__ == '__main__':
    import scriptine

    scriptine.run()