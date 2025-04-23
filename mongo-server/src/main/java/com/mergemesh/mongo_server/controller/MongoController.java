package com.mergemesh.mongo_server.controller;

import com.mergemesh.mongo_server.service.MongoService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/mongo")
public class MongoController {

    private final MongoService mongoService;

    public MongoController(MongoService mongoService) {
        this.mongoService = mongoService;
    }

    @GetMapping
    public String getGrade(@RequestParam String studentId, @RequestParam String courseId) {
        return mongoService.getGrade(studentId, courseId);
    }

    @PostMapping
    public String updateGrade(@RequestBody Map<String, String> req) {
        mongoService.updateGrade(req);
        return "Mongo grades updated successfully.";
    }
}
