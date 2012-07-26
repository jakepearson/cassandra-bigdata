package com.rallydev.cassandra;

public interface KeyValueStore {
    String COLUMN_FAMILY = "BigDataTestFamily";
    String KEYSPACE_NAME = "BigDataTestKeyspace";

    void deleteKeyspace();

    String get(String key);

    boolean put(String key, String value);

    boolean delete(String key);

    void readAllRowsAndThen(RowRunnable andThen);
}
