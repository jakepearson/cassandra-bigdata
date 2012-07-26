package com.rallydev.cassandra;

import me.prettyprint.hector.api.beans.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ActionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActionRunner.class);

    KeyValueStore store = new HectorKeyValueStore();
    private int counter;

    LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 25, 10, TimeUnit.SECONDS, queue);

    public void create() {
        counter = 0;
        int count = 10000000;
        for(int i = 1; i<=count; i++) {
            buildCreate(i, count);
        }
        LOGGER.info("Created {} rows", counter);
    }

    private void buildCreate(final int pos, final int count) {
        limitQueueSize();
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if (store.put(UUID.randomUUID().toString(), UUID.randomUUID().toString())) {
                    counter++;
                }
                if (pos % 1000 == 0) {
                    LOGGER.info("Writing ({}/{})", pos, count);
                }
            }
        });
    }

    public void delete() {
        counter = 0;
        store.readAllRowsAndThen(new RowRunnable() {
            public void run(Row<String, Long, String> row) {
                buildDelete(row, counter);
                counter++;
            }
        });
        LOGGER.info("Deleted {} rows", counter);
    }

    private void buildDelete(final Row<String, Long, String> row, final int pos) {
        limitQueueSize();
        executor.submit(new Runnable() {
            @Override
            public void run() {
                store.delete(row.getKey());
                if(pos % 1000 == 0) {
                    LOGGER.info("Deleted ({})", pos);
                }
            }
        });
    }

    public void read() {
        counter = 0;
        store.readAllRowsAndThen(new RowRunnable() {
            public void run(Row<String, Long, String> row) {
                //LOGGER.info(row.getKey());
                counter++;
                if(counter % 1000 == 0) {
                    LOGGER.info("Read ({})", counter);
                }
            }
        });
        LOGGER.info("Read {} rows", counter);
    }

    public void reset() {
        store.deleteKeyspace();
    }

    public void exercise() {
        long start = (new Date()).getTime();
        create();
        waitForComplete();
        long createEnd = (new Date()).getTime();
        read();
        long readEnd = (new Date()).getTime();
        delete();
        waitForComplete();
        long deleteEnd = (new Date()).getTime();

        LOGGER.info("Create took {} seconds", (createEnd - start)/1000);
        LOGGER.info("Read took {} seconds", (readEnd - createEnd)/1000);
        LOGGER.info("Delete took {} seconds", (deleteEnd - readEnd)/1000);

        executor.shutdown();
    }

    private void limitQueueSize() {
        if (executor.getQueue().size() > 100000) {
            //don't let the pool size grow too big
            waitForComplete();
        }
    }

    private void waitForComplete() {
        long sleep = 100;
        while (true) {
            if (executor.getQueue().size() == 0) {
                break;
            } else {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
