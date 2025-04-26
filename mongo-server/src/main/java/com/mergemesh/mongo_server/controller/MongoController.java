package com.mergemesh.mongo_server.controller;

import com.mergemesh.mongo_server.service.MongoService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


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

    @PutMapping
    public String updateGrade(@RequestBody Map<String, String> req) {
        mongoService.updateGrade(req);
        return "Mongo grades updated successfully.";
    }

    @PostMapping
    public String insertGrade(@RequestBody Map<String, String> req) {
        mongoService.insertGrade(req);
        return "Mongo grades inserted successfully";
    }
    
}
