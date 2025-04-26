package com.mergemesh.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveConfig {
    private final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private final String HIVE_URL = "jdbc:hive2://localhost:10000"; // Hive JDBC connection URL
    private Connection connection;

    public HiveConfig() {}

    public Connection getHiveConnection() throws SQLException {
        if (connection == null) {
            try {
                Class.forName(DRIVER_NAME);
            } catch (ClassNotFoundException e) {
                System.out.println("Driver not found!");
                e.printStackTrace();
            }
            connection = DriverManager.getConnection(HIVE_URL);

            Statement stmt = connection.createStatement();
            // Set required Hive session properties for ACID support
            stmt.execute("SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
            stmt.execute("SET hive.support.concurrency=true");
            stmt.execute("SET hive.enforce.bucketing=true");
            stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict");
            stmt.execute("SET hive.compactor.initiator.on=true");
            stmt.execute("SET hive.compactor.worker.threads=1");

            System.out.println("Connected to Hive server successfully!");
        }

        return connection;
    }
}
