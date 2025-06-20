package com.mergemesh.mongo_server.controller;

import com.mergemesh.mongo_server.service.MongoService;

import com.mergemesh.shared.OplogEntry;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@RestController
public class MongoController {

    private final MongoService mongoService;

    public MongoController(MongoService mongoService) {
        this.mongoService = mongoService;
    }

    @GetMapping
    public String getGrade(@RequestParam String studentId, @RequestParam String courseId) {
        return mongoService.getGrade(studentId, courseId);
    }

    @PutMapping
    public String updateGrade(@RequestBody Map<String, String> req) {
        String localDateTime = LocalDateTime.now().toString();
        req.put("timestamp",localDateTime);
        mongoService.updateGrade(req);
        return "Mongo grades updated successfully.";
    }

    @PostMapping
    public String insertGrade(@RequestBody Map<String, String> req) {
        String localDateTime = LocalDateTime.now().toString();
        req.put("timestamp",localDateTime);
        mongoService.insertGrade(req);
        return "Mongo grades inserted successfully";
    }

    @GetMapping("/logs")
    public List<OplogEntry> getLogValues() {
        return mongoService.getLogValues();
    }

    @PostMapping("/merge")
    public String merge(@RequestBody Map<String, String> req) {
        mongoService.merge(req.get("server"));
        return "Merge completed successfully.";
    }

}
