package com.mergemesh.postgres_server.controller;

import com.mergemesh.postgres_server.service.PostgresService;
import com.mergemesh.shared.OplogEntry;

import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
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
    @PostMapping
    public String insertMarks(@RequestBody Map<String, String> req) {
        req.put("timestamp", LocalDateTime.now().toString());
        try {
            postgresService.insertGrade(req);
            return "Grades inserted successfully.";
        } catch (SQLException e) {
            return "Failed to insert grades: " + e.getMessage();
        }
    }

    @PutMapping
    public String updateMarks(@RequestBody Map<String, String> req) {
        req.put("timestamp", LocalDateTime.now().toString());
        try {
            postgresService.updateGrade(req);
            return "Grades updated successfully.";
        } catch (SQLException e) {
            return "Failed to update grades: " + e.getMessage();
        }
    }

    @GetMapping("/logs")
    public List<OplogEntry> getLogValues() {
        return postgresService.getLogValues();
    }

    @PostMapping("/merge")
    public String merge(@RequestBody Map<String, String> req) {
        postgresService.merge(req.get("server"));
        return "Merge completed successfully.";
    }
}
