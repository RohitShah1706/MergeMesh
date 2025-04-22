package com.mergemesh.postgres_server.service;

import com.mergemesh.postgres_server.shared.OplogEntry;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Map;

@Service
public class PostgresService {

    private final Connection conn;
    private final LoggerService loggerService;

    public PostgresService(Connection conn, LoggerService loggerService) throws SQLException {
        this.conn = conn;
        this.loggerService = loggerService;
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
}
