package com.rallydev.cassandra;

import me.prettyprint.hector.api.beans.Row;

public interface RowRunnable {
    public void run(Row<String, Long, String> row);
}
