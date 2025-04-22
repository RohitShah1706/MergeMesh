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
