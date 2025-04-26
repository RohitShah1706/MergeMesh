package com.mergemesh.postgres_server.service;

import com.mergemesh.shared.OplogEntry;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@Service
public class PostgresService {

    private final Connection conn;
    private final RestTemplate restTemplate;
    private final LoggerService loggerService;

    public PostgresService(Connection conn, RestTemplate restTemplate, LoggerService loggerService)
            throws SQLException {
        this.conn = conn;
        this.restTemplate = restTemplate;
        this.loggerService = loggerService;
    }

    public void insertGrade(Map<String, String> req) throws SQLException {
        String studentId = req.get("studentId");
        String courseId = req.get("courseId");
        String grade = req.get("grade");
        String sql = "INSERT INTO graderoster (student_id, course_id, grade) VALUES (?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, studentId);
            ps.setString(2, courseId);
            ps.setString(3, grade);
            ps.executeUpdate();

            OplogEntry oplogEntry = new OplogEntry("INSERT", "graderoster", req, LocalDateTime.now().toString());
            loggerService.logToFile(oplogEntry.toString());
        }
    }

    public void updateGrade(Map<String, String> req) throws SQLException {
        String studentId = req.get("studentId");
        String courseId = req.get("courseId");
        String grade = req.get("newGrade");
        String sql = "UPDATE graderoster SET grade = ? WHERE student_id = ? AND course_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, grade);
            ps.setString(2, studentId);
            ps.setString(3, courseId);
            ps.executeUpdate();

            OplogEntry oplogEntry = new OplogEntry("UPDATE", "graderoster", req, LocalDateTime.now().toString());
            loggerService.logToFile(oplogEntry.toString());
        }
    }

    public String getGrade(String studentId, String courseId) throws SQLException {
        String sql = "SELECT grade FROM graderoster WHERE student_id = ? AND course_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, studentId);
            ps.setString(2, courseId);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? rs.getString(1) : "F";
        }
    }

    public void merge(String server) {
        try {
            Map<String, OplogEntry> gradeMapSelf = new HashMap<>();
            Map<String, OplogEntry> gradeMapRemote = new HashMap<>();

            // Read local log file
            List<OplogEntry> localLog = loggerService.readLogFile();
            for (OplogEntry entry : localLog) {
                if ("graderoster".equals(entry.getTableName())) {
                    String key = entry.getData().get("studentId") + "_" + entry.getData().get("courseId");
                    if ("INSERT".equals(entry.getOperation()) || "UPDATE".equals(entry.getOperation())) {
                        gradeMapSelf.put(key, entry);
                    }
                }
            }

            // Read remote log file
            List<OplogEntry> remoteLog = readRemoteLogFile(server);
            for (OplogEntry entry : remoteLog) {
                if ("graderoster".equals(entry.getTableName())) {
                    String key = entry.getData().get("studentId") + "_" + entry.getData().get("courseId");
                    if ("INSERT".equals(entry.getOperation()) || "UPDATE".equals(entry.getOperation())) {
                        gradeMapRemote.put(key, entry);
                    }
                }
            }

            // Apply changes to the database
            for (Map.Entry<String, OplogEntry> entry : gradeMapRemote.entrySet()) {
                String[] keys = entry.getKey().split("_");
                String studentIdRemote = keys[0];
                String courseIdRemote = keys[1];
                String gradeRemote;
                String operation = entry.getValue().getOperation();
                if (operation.equals("INSERT")) {
                    gradeRemote = entry.getValue().getData().get("grade");
                } else if (operation.equals("UPDATE")) {
                    gradeRemote = entry.getValue().getData().get("newGrade");
                } else {
                    gradeRemote = "Not Available";
                }
                Map<String, String> data = new HashMap<>();
                data.put("studentId", studentIdRemote);
                data.put("courseId", courseIdRemote);

                OplogEntry selfEntry = gradeMapSelf.getOrDefault(entry.getKey(), null);
                if (selfEntry == null) {
                    data.put("grade", gradeRemote);
                    insertGrade(data);
                } else {
                    // If the entry exists in both logs, update it
                    LocalDateTime selfTimestamp = LocalDateTime.parse(selfEntry.getTimestamp());
                    LocalDateTime remoteTimestamp = LocalDateTime.parse(entry.getValue().getTimestamp());
                    if (selfTimestamp.compareTo(remoteTimestamp) < 0) {
                        data.put("newGrade", gradeRemote);
                        updateGrade(data);
                    }
                }

            }
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
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

    private List<OplogEntry> readRemoteLogFile(String server) {
        String URL = server + "/logs";
        ResponseEntity<OplogEntry[]> response = restTemplate.getForEntity(URL, OplogEntry[].class);
        List<OplogEntry> responseBody = Arrays.asList(response.getBody());

        // System.out.println("Grade from hive service: " + responseBody);
        return responseBody;
    }

    private void tryQueryOtherBackend() {
        String URL = "http://localhost:8081?studentId=SID7072&courseId=CSE007";
        ResponseEntity<String> response = restTemplate.getForEntity(URL, String.class);

        String responseBody = response.getBody();
        System.out.println("Grade from hive service: " + responseBody);
    }
}
