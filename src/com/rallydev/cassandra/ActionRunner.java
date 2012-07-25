package com.rallydev.cassandra;

import me.prettyprint.hector.api.beans.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ActionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActionRunner.class);

    HectorKeyValueStore store = new HectorKeyValueStore();

    public void create() {
        int count = 200;
        for(int i = 1; i<=count; i++) {
            store.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            if(i % 100 == 0) {
                LOGGER.info("Writing (" + i + "/" + count + ")");
            }
        }
    }

    public void delete() {
        store.readAllRowsAndThen(new RowRunnable() {
            public void run(Row<String, Long, String> row) {
                //TODO delete?
            }
        });
    }


    public void read() {
        store.readAllRowsAndThen(new RowRunnable() {
            public void run(Row<String, Long, String> row) {
                LOGGER.info(row.getKey());
            }
        });
    }

    public void reset() {
        store.deleteKeyspace();
    }
}
