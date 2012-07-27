package com.rallydev.cassandra;

public interface KeyValueStore {
    String COLUMN_FAMILY = "BigDataTestFamily";
    String KEYSPACE_NAME = "BigDataTestKeyspace";

    String get(String key, String tableName);
    boolean put(String key, String value, String tableName);
    boolean delete(String key, String tableName);

    void readAllRowsAndThen(String tableName, RowRunnable andThen);

    boolean containsTable(String tableName);
    void createTable(String tableName);
    void deleteTable(String tableName);

    boolean containsKeyspace(String keyspace);
    void createKeyspace(String keyspace);
    void deleteKeyspace();
}
