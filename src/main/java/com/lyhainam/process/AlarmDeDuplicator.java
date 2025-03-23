package com.lyhainam.process;

import com.lyhainam.util.DatabaseUtil;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AlarmDeDuplicator {
    private static final String DB_HOST = "127.0.0.1";
    private static final Integer DB_PORT = 3306;
    private static final String DB_NAME = "alarm_db";
    private static final String DB_USERNAME = "root";
    private static final String DB_PASSWORD = "password";
    private static final int THREAD_POOL_SIZE = 5;
    private final ExecutorService executorService;
    private final long deDuplicationWindow;
//    private final MLModel mlModel;

    public AlarmDeDuplicator(long deDuplicationWindow) {
        this.deDuplicationWindow = deDuplicationWindow;
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    }

    private String generateHash(String alarmType, String device, String severity) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            String key = alarmType + "#" + device + "#" + severity;
            byte[] hash = messageDigest.digest(key.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch(Exception e) {
            throw new RuntimeException("Error generating hash", e);
        }
    }

    public void processAlarm(String alarmType, String device, String severity) {
        executorService.submit(() -> handleAlarm(alarmType, device, severity));
    }

    private void handleAlarm(String alarmType, String device, String severity) {
        long timestamp = System.currentTimeMillis();
        try (Connection conn = DatabaseUtil.getConnection(DatabaseUtil.DatabaseType.MARIADB, DB_HOST, DB_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD)) {
            // use transaction
            conn.setAutoCommit(false);

            String selectSQL = "SELECT timestamp, count FROM alarms WHERE alarm_type=? AND device=? AND severity FOR UPDATE";
            try (PreparedStatement selectStmt = conn.prepareStatement(selectSQL)) {
                selectStmt.setString(1, alarmType);
                selectStmt.setString(2, device);
                selectStmt.setString(3, severity);
                ResultSet rs = selectStmt.executeQuery();

                if (rs.next()) {
                    long lastTimestamp = rs.getLong("timestamp");
                    int count = rs.getInt("count");

                    if (timestamp - lastTimestamp <= deDuplicationWindow) {
                        // Update counter if the alarm is treated as duplicate
                        String updateSQL = "UPDATE alarms SET count = count + 1 WHERE alarm_type=? AND device=? AND severity=?";
                        try (PreparedStatement updateStmt = conn.prepareStatement(updateSQL)) {
                            updateStmt.setString(1, alarmType);
                            updateStmt.setString(2, device);
                            updateStmt.setString(3, severity);
                            updateStmt.executeUpdate();
                        }
                        System.out.println("[DEDUPLICATED] Alarm '" + alarmType + "' from '" + device + "' - Count: " + (count + 1));
                    } else {
                        insertNewAlarm(conn, alarmType, device, severity, timestamp);
                    }
                } else {
                    insertNewAlarm(conn, alarmType, device, severity, timestamp);
                }
            }
            // commit transaction
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertNewAlarm(Connection conn, String alarmType, String device, String severity, long timestamp) throws SQLException {
        String insertSQL = "INSERT INTO alarms (alarm_type, device, severity, timestamp, count) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
            insertStmt.setString(1, alarmType);
            insertStmt.setString(2, device);
            insertStmt.setString(3, severity);
            insertStmt.setLong(4, timestamp);
            insertStmt.setInt(5, 1);
            insertStmt.executeUpdate();
        }
        System.out.println("[NEW] Alarm '" + alarmType + "' from '" + device + "' logged.");
    }
    public void shutdown() {
        executorService.shutdown();
    }

    public static void main(String[] args) {
        AlarmDeDuplicator processor = new AlarmDeDuplicator(20000);

        for (int i = 0; i < 10; i++) {
            processor.processAlarm("Link Failure", "Router A", "Critical");
        }

        processor.shutdown();
    }
}
