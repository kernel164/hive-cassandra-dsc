hive-cassandra-dsc
==================

Hive Cassandra StorageHandler using datastax-java-driver 

Supports:
========

- To create external hive table that points to cassandra columnfamily (with where clause support to fetch a subset of data from cassandra)

Example:
========

```
CREATE EXTERNAL TABLE IF NOT EXISTS kvtable1 (k bigint, v bigint) STORED BY 'org.apache.cassandra.hive.cql3.Cql3StorageHandler' WITH SERDEPROPERTIES ("hosts" = "cass01,cass02", "ks" = "keyspace1", "cf" = "kvtable1", "where" = "k > 1000 and k < 9000")
CREATE TABLE IF NOT EXISTS kvtable1hive AS SELECT * FROM kvtable1 WHERE k > 2000 & k < 5000
SELECT k, count(*) AS groupcount FROM kvtable1hive GROUP BY k HAVING groupcount > 1
```

Pending features:
- Add support to load data from hive to cassandra
- Add support to create cassandra tables.