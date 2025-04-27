package com.mergemesh.postgres_server.service;

import com.mergemesh.shared.OplogEntry;

import com.mergemesh.shared.Server;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@Service
public class PostgresService implements Server {

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
