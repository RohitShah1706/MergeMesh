package com.mergemesh.hive_server.service;

import com.mergemesh.hive_server.config.HiveConfig;
import com.mergemesh.hive_server.config.RestConfig;
import com.mergemesh.shared.OplogEntry;
import com.mergemesh.shared.Server;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.*;

public class HiveService implements Server {

    private final HiveConfig hiveConfig;
    private final RestConfig restConfig;
    private final LoggerService loggerService;

    public HiveService() {
        this.hiveConfig = new HiveConfig();
        this.restConfig = new RestConfig();
        this.loggerService = new LoggerService();
    }

    public Connection getHiveConnection() {
        Connection connection = null;
        try {
            connection = hiveConfig.getHiveConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return connection;
    }

    public RestTemplate getRestTemplate() {
        return this.restConfig.getRestTemplate();
    }

    public String getGrade(String studentId, String courseId) {
        Connection connection = getHiveConnection();
        try {
            PreparedStatement sql = connection
                    .prepareStatement("SELECT grade FROM graderoster WHERE student_id = ? AND course_id = ?");
            sql.setString(1, studentId);
            sql.setString(2, courseId);

            ResultSet rs = sql.executeQuery();

            if (!rs.next()) {
                return null;
            }

            String grade = rs.getString("grade");
            return grade;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void updateGrade(Map<String, String> req) {
        String studentId = req.get("studentId");
        String courseId = req.get("courseId");
        String grade = req.get("newGrade");
        String timestamp = req.get("timestamp");
        req.remove("timestamp");

        Connection connection = getHiveConnection();

        try {
            PreparedStatement sql = connection
                    .prepareStatement("UPDATE graderoster SET grade = ? WHERE student_id = ? AND course_id = ?");
            sql.setString(1, grade);
            sql.setString(2, studentId);
            sql.setString(3, courseId);

            sql.executeUpdate();

            OplogEntry oplogEntry = new OplogEntry("UPDATE", "graderoster", req, timestamp);
            loggerService.logToFile(oplogEntry.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertGrade(Map<String, String> req) {
        String studentId = req.get("studentId");
        String courseId = req.get("courseId");
        String grade = req.get("grade");
        String timestamp = req.get("timestamp");
        req.remove("timestamp");

        Connection connection = getHiveConnection();

        try {
            PreparedStatement sql = connection
                    .prepareStatement("INSERT INTO graderoster (student_id, course_id, grade) VALUES (?, ?, ?)");
            sql.setString(1, studentId);
            sql.setString(2, courseId);
            sql.setString(3, grade);

            sql.executeUpdate();

            OplogEntry oplogEntry = new OplogEntry("INSERT", "graderoster", req, timestamp);
            loggerService.logToFile(oplogEntry.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<OplogEntry> getLogValues() {
        List<OplogEntry> logs = loggerService.readLogFile();
        Map<String, OplogEntry> logMap = new HashMap<>(); // (sid, cid) -> oplogentry
        for (OplogEntry entry : logs) {
            if ("graderoster".equals(entry.getTableName())) {
                String studentId = entry.getData().get("studentId");
                String courseId = entry.getData().get("courseId");
                logMap.put(studentId + "_" + courseId, entry);
            }
        }

        // returns the last update OR insert oplog entries for each (sid, cid) pair.
        return new ArrayList<>(logMap.values());
    }

    public List<OplogEntry> readRemoteLogFile(String server) {
        String URL = URI.create(server.trim() + "/logs").toString();
        RestTemplate restTemplate = getRestTemplate();
        ResponseEntity<OplogEntry[]> response = restTemplate.getForEntity(URL, OplogEntry[].class);
        List<OplogEntry> responseBody = Arrays.asList(response.getBody());

        // System.out.println("Grade from " + server + ": " + responseBody);
        return responseBody;
    }

    private void tryQueryOtherBackend() {
        RestTemplate restTemplate = getRestTemplate();
        String URL = "http://localhost:5000/?studentId=SID7072&courseId=CSE007";
        ResponseEntity<String> response = restTemplate.getForEntity(URL, String.class);

        String responseBody = response.getBody();
        System.out.println("Grade from postgres service: " + responseBody);
    }
}
