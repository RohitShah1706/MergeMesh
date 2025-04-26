package com.mergemesh.postgres_server.controller;

import com.mergemesh.postgres_server.service.PostgresService;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.Map;

@RestController
public class PostgresController {

    private final PostgresService postgresService;

    public PostgresController(PostgresService postgresService) {
        this.postgresService = postgresService;
    }

    @GetMapping()
    public String getMarks(@RequestParam String studentId, @RequestParam String courseId) throws SQLException {
        return postgresService.getGrade(studentId, courseId);
    }

    // SID, CID, GRADE
    // Example: {"studentId": "123", "courseId": "456", "grade": "A"}
    @PostMapping("/insert")
    public String insertMarks(@RequestBody Map<String, String> req) {
        try {
            postgresService.insertGrade(req);
            return "Grades inserted successfully.";
        } catch (SQLException e) {
            return "Failed to insert grades: " + e.getMessage();
        }
    }


    @PostMapping
    public String updateMarks(@RequestBody Map<String, String> req) {
        try {
            postgresService.updateGrade(req);
            return "Grades updated successfully.";
        } catch (SQLException e) {
            return "Failed to update grades: " + e.getMessage();
        }
    }
}
