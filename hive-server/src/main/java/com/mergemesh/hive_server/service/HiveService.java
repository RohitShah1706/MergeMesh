package com.mergemesh.hive_server.service;

import com.mergemesh.hive_server.config.HiveConfig;
import com.mergemesh.shared.OplogEntry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Map;

public class HiveService {

    private final HiveConfig hiveConfig;
    private final LoggerService loggerService;

    public HiveService() {
        this.hiveConfig = new HiveConfig();
        this.loggerService = new LoggerService();
    }

    public Connection getHiveConnection() {
        Connection connection = null;
        try {
            connection = hiveConfig.getHiveConnection();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return connection;
    }

    public String getGrade(String studentId, String courseId) {
        Connection connection = getHiveConnection();
        try {
            PreparedStatement sql = connection.prepareStatement("SELECT grade FROM graderoster WHERE student_id = ? AND course_id = ?");
            sql.setString(1, studentId);
            sql.setString(2, courseId);

            ResultSet rs = sql.executeQuery();

            if(!rs.next()) {
                return null;
            }

            String grade = rs.getString("grade");
            return grade;
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void updateGrade(Map<String, String> req) {
        String studentId = req.get("studentId");
        String courseId = req.get("courseId");
        String grade = req.get("newGrade");

        Connection connection = getHiveConnection();

        try {
            PreparedStatement sql = connection.prepareStatement("UPDATE graderoster SET grade = ? WHERE student_id = ? AND course_id = ?");
            sql.setString(1, grade);
            sql.setString(2, studentId);
            sql.setString(3, courseId);

            sql.executeUpdate();

            OplogEntry oplogEntry = new OplogEntry("UPDATE", "graderoster", req, LocalDateTime.now().toString());
            loggerService.logToFile(oplogEntry.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertGrade(Map<String, String> req) {
        String studentId = req.get("studentId");
        String courseId = req.get("courseId");
        String grade = req.get("grade");

        Connection connection = getHiveConnection();

        try {
            PreparedStatement sql = connection.prepareStatement("INSERT INTO graderoster (student_id, course_id, grade) VALUES (?, ?, ?)");
            sql.setString(1, studentId);
            sql.setString(2, courseId);
            sql.setString(3, grade);

            sql.executeUpdate();

            OplogEntry oplogEntry = new OplogEntry("INSERT", "graderoster", req, LocalDateTime.now().toString());
            loggerService.logToFile(oplogEntry.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
