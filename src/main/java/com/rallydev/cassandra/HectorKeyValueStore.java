package com.rallydev.cassandra;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static java.lang.System.getProperty;
import static me.prettyprint.hector.api.HConsistencyLevel.QUORUM;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

public class HectorKeyValueStore implements KeyValueStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(HectorKeyValueStore.class);
    private static final StringSerializer STRING_SERIALIZER = StringSerializer.get();
    private static final LongSerializer LONG_SERIALIZER = LongSerializer.get();
    private static final String HOSTS = "cas2cluster1:9160, cas2cluster2:9160, cas2cluster3:9160";
    private static final Cluster CLUSTER = getOrCreateCluster(getProperty("CLUSTER_NAME", "zuul-cluster"), new CassandraHostConfigurator(HOSTS));
    private final ConfigurableConsistencyLevel POLICY;
    private final Keyspace KEYSPACE;

    public HectorKeyValueStore() {
        POLICY = new ConfigurableConsistencyLevel();
        POLICY.setDefaultReadConsistencyLevel(QUORUM);
        POLICY.setDefaultWriteConsistencyLevel(QUORUM);

        if(CLUSTER.describeKeyspace(KEYSPACE_NAME) == null) {
            KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition(KEYSPACE_NAME);
            CLUSTER.addKeyspace(keyspaceDefinition);
        }

        KEYSPACE = HFactory.createKeyspace(KEYSPACE_NAME, CLUSTER, POLICY);

        if (CLUSTER.describeKeyspace(KEYSPACE_NAME).getCfDefs().size() == 0) {
            ColumnFamilyDefinition definition = HFactory.createColumnFamilyDefinition(KEYSPACE.getKeyspaceName(), COLUMN_FAMILY);
            CLUSTER.addColumnFamily(definition);
        }
    }

    @Override
    public void deleteKeyspace() {
        try {
            CLUSTER.dropKeyspace(KEYSPACE.getKeyspaceName());
        } catch (HInvalidRequestException e) {
            System.out.println(e);
        }
    }

    @Override
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
    
    @Override
    public boolean put(final String key, final String value) {
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

    @Override
    public boolean delete(final String key) {
        try {
            Mutator<String> mutator = HFactory.createMutator(KEYSPACE, STRING_SERIALIZER);
            mutator.addDeletion(key, COLUMN_FAMILY);
            mutator.execute();
            return true;
        } catch (Exception e) {
            LOGGER.error("Delete failed", e);
            return false;
        }
    }

    @Override
    public void readAllRowsAndThen(RowRunnable andThen) {
        int sliceCount = 100;
        RangeSlicesQuery<String, Long, String> rangeSlicesQuery = HFactory
                .createRangeSlicesQuery(KEYSPACE, STRING_SERIALIZER, LONG_SERIALIZER, STRING_SERIALIZER)
                .setColumnFamily(COLUMN_FAMILY)
                .setRange(null, null, false, 10)
                .setRowCount(sliceCount);

        String last_key = null;

        while (true) {
            rangeSlicesQuery.setKeys(last_key, null);

            QueryResult<OrderedRows<String, Long, String>> result = rangeSlicesQuery.execute();
            OrderedRows<String, Long, String> rows = result.get();
            Iterator<Row<String, Long, String>> rowsIterator = rows.iterator();

            // we'll skip this first one, since it is the same as the last one from previous time we executed
            if (last_key != null && rowsIterator != null) rowsIterator.next();

            while (rowsIterator.hasNext()) {
                Row<String, Long, String> row = rowsIterator.next();
                last_key = row.getKey();

                if (row.getColumnSlice().getColumns().isEmpty()) {
                    continue;
                }

                //execute logic
                andThen.run(row);
            }

            if (rows.getCount() < sliceCount) {
                break;
            }
        }
    }
}
