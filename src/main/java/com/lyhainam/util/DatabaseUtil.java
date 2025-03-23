package com.lyhainam.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseUtil {
    public enum DatabaseType {
        MYSQL, ORACLE, MARIADB, CLICKHOUSE, POSTGRESQL, PRESTO, HIVE
    }

    public static Connection getConnection(DatabaseType dbType, String host, int port, String database, String user, String password) throws SQLException {
        String url = "";

        switch (dbType) {
            case MYSQL:
                url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useSSL=false&serverTimezone=UTC";
                break;
            case POSTGRESQL:
                url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
                break;
            case MARIADB:
                url = "jdbc:mariadb://" + host + ":" + port + "/" + database;
                break;
            case CLICKHOUSE:
                url = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
                break;
            case ORACLE:
                url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + database;
                break;
            case PRESTO:
                url = "jdbc:presto://" + host + ":" + port + "/" + database;
                break;
            case HIVE:
                url = "jdbc:hive2://" + host + ":" + port + "/" + database;
                break;
            default:
                throw new IllegalArgumentException("Unsupported database type");
        }

        return DriverManager.getConnection(url, user, password);
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.fillInStackTrace();
            }
        }
    }
}
