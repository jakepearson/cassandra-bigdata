package com.rallydev.cassandra;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

import static java.lang.System.getProperty;
import static me.prettyprint.hector.api.HConsistencyLevel.QUORUM;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

public class HectorKeyValueStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(HectorKeyValueStore.class);
    private static final StringSerializer STRING_SERIALIZER = StringSerializer.get();
    private static final LongSerializer LONG_SERIALIZER = LongSerializer.get();
    public static final String COLUMN_FAMILY = "BigDataTestFamily";
    public static final String ZUUL_KEYSPACE_NAME = "BigDataTestKeyspace";
    public static final String HOSTS = getProperty("CASSANDRA_HOSTS", "localhost:" + getProperty("cassandra.rpc_port", "9170"));
    public static final Cluster CLUSTER = getOrCreateCluster(getProperty("CLUSTER_NAME", "zuul-cluster"), new CassandraHostConfigurator(HOSTS));
    public static final ConfigurableConsistencyLevel POLICY;
    public static final Keyspace KEYSPACE;

    static {
        POLICY = new ConfigurableConsistencyLevel();
        POLICY.setDefaultReadConsistencyLevel(QUORUM);

        POLICY.setDefaultWriteConsistencyLevel(QUORUM);
        KEYSPACE = HFactory.createKeyspace(ZUUL_KEYSPACE_NAME, CLUSTER, POLICY);
    }

    public String get(String key) {
        final SliceQuery<String, Long, String> query = HFactory.createSliceQuery(KEYSPACE, STRING_SERIALIZER, LONG_SERIALIZER, STRING_SERIALIZER);
        query.setKey(key);
        query.setColumnFamily(COLUMN_FAMILY);

        QueryResult<ColumnSlice<Long, String>> execute = query.execute();
        List<HColumn<Long,String>> columns = execute.get().getColumns();
        if (columns == null || columns.size() == 0) return null;
        HColumn<Long, String> column = columns.get(0);
        return column.getValue();
    }
    
    public Boolean put(final String key, final String value) {
        try {
            Mutator<String> mutator = HFactory.createMutator(KEYSPACE, STRING_SERIALIZER);
            mutator.addInsertion(key, COLUMN_FAMILY, HFactory.createColumn((new Date()).getTime(), value, LONG_SERIALIZER, STRING_SERIALIZER));
            mutator.execute();
            return true;
        } catch (Exception e) {
            LOGGER.error("Insertion failed", e);
            return false;
        }
    }
}
