package com.mergemesh.hive_server.controller;

import com.mergemesh.hive_server.service.HiveService;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.Map;

@RestController
public class HiveController {

    private final HiveService hiveService;

    public HiveController(HiveService hiveService) {
        this.hiveService = hiveService;
    }

    @GetMapping()
    public String getMarks(@RequestParam String studentId, @RequestParam String courseId) throws SQLException {
        return hiveService.getGrade(studentId, courseId);
    }

    @PostMapping
    public String updateMarks(@RequestBody Map<String, String> req) {
        try {
            hiveService.updateGrade(req);
            return "Grades updated successfully.";
        } catch (SQLException e) {
            return "Failed to update grades: " + e.getMessage();
        }
    }
}

