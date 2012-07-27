package com.rallydev.cassandra;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CqlKeyValueStore implements KeyValueStore {

    @Override
    public void deleteKeyspace() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String get(String key, String tableName) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean put(String key, String value, String tableName) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(String key, String tableName) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsTable(String tableName) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void createTable(String tableName) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void deleteTable(String tableName) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readAllRowsAndThen(String tableName, RowRunnable andThen) {
        Connection con = null;
        try {
            Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
            //con = DriverManager.getConnection("jdbc:cassandra:root/root@localhost:9160/MyKeyspace");
            con = DriverManager.getConnection("jdbc:cassandra://cas2cluster1:9160/" + KEYSPACE_NAME);

            String query = "SELECT * FROM " + tableName;

            Statement stmt = con.createStatement();
            ResultSet result = stmt.executeQuery(query);

            while (result.next()) {
                String key = new String(result.getBytes(1));
                String value = new String(result.getBytes(2));

                System.out.println(key + ":" + value);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                con = null;
            }
        }
    }
}
