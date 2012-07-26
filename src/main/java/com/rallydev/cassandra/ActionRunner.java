package com.rallydev.cassandra;

import me.prettyprint.hector.api.beans.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ActionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActionRunner.class);

    HectorKeyValueStore store = new HectorKeyValueStore();
    private int counter;

    public void create() {
        counter = 0;
        int count = 200;
        for(int i = 1; i<=count; i++) {
            if (store.put(UUID.randomUUID().toString(), UUID.randomUUID().toString())) {
                counter++;
            }
            if(i % 100 == 0) {
                LOGGER.info("Writing (" + i + "/" + count + ")");
            }
        }
        LOGGER.info("Created {} rows", counter);
    }

    public void delete() {
        counter = 0;
        store.readAllRowsAndThen(new RowRunnable() {
            public void run(Row<String, Long, String> row) {
                if (store.delete(row.getKey())) {
                    counter++;
                }
            }
        });
        LOGGER.info("Deleted {} rows", counter);
    }


    public void read() {
        counter = 0;
        store.readAllRowsAndThen(new RowRunnable() {
            public void run(Row<String, Long, String> row) {
                LOGGER.info(row.getKey());
                counter++;
            }
        });
        LOGGER.info("Read {} rows", counter);
    }

    public void reset() {
        store.deleteKeyspace();
    }
}
