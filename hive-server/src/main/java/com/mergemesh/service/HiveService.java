package com.mergemesh.service;

import com.mergemesh.config.HiveConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

public class HiveService {

    private HiveConfig hiveConfig;

    public HiveService() {
        this.hiveConfig = new HiveConfig();
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
