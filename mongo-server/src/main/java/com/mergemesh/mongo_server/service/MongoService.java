package com.mergemesh.mongo_server.service;

import com.mergemesh.shared.OplogEntry;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.time.LocalDateTime;
import java.util.Map;

import org.bson.Document;
import org.springframework.stereotype.Service;

@Service
public class MongoService {

    private final MongoCollection<Document> collection;
    private final LoggerService loggerService;

    public MongoService(MongoClient mongoClient,LoggerService loggerService) {
        MongoDatabase database = mongoClient.getDatabase("mergemesh");
        this.collection = database.getCollection("graderoster");

        this.loggerService = loggerService;
    }

    public String getGrade(String studentId, String courseId) {
        Document query = new Document("student-ID", studentId).append("course-id", courseId);
        Document result = collection.find(query).first();
        
        return result != null ? result.getString("grade") : "Grade Not Available";
    }

    public void updateGrade(Map<String, String> req) {
        String studentId = req.get("studentId");
        String courseId = req.get("courseId");
        String grade = req.get("newGrade");

        Document query = new Document("student-ID", studentId).append("course-id", courseId);
        Document update = new Document("$set", new Document("grade", grade));
        collection.updateOne(query, update);

        OplogEntry oplogEntry = new OplogEntry("UPDATE", "graderoster", req, LocalDateTime.now().toString());
        loggerService.logToFile(oplogEntry.toString());
    }
}
