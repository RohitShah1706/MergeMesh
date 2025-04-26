package com.mergemesh.mongo_server.service;

import com.mergemesh.shared.OplogEntry;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class MongoService {

    private final MongoCollection<Document> collection;
    private final RestTemplate restTemplate;
    private final LoggerService loggerService;

    public MongoService(MongoClient mongoClient, RestTemplate restTemplate, LoggerService loggerService) {
        MongoDatabase database = mongoClient.getDatabase("mergemesh");
        this.collection = database.getCollection("graderoster");
        this.restTemplate = restTemplate;
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

    public void insertGrade(Map<String, String> req) {
        String studentId = req.get("studentId");
        String courseId = req.get("courseId");
        String grade = req.get("grade");
    
        // Create a new document to insert
        Document doc = new Document("student-ID", studentId)
                            .append("course-id", courseId)
                            .append("grade", grade);
    
        // Insert the document into the collection
        collection.insertOne(doc);
    
        // Log the operation
        OplogEntry oplogEntry = new OplogEntry("INSERT", "graderoster", req, LocalDateTime.now().toString());
        loggerService.logToFile(oplogEntry.toString());
    }

    public List<OplogEntry> getLogValues() {
        List<OplogEntry> logs = loggerService.readLogFile();
        Map<String, OplogEntry> logMap = new HashMap<>(); // (sid, cid) -> oplogentry
        for (OplogEntry entry : logs) {
            if ("graderoster".equals(entry.getTableName())) {
                String studentId = entry.getData().get("studentId");
                String courseId = entry.getData().get("courseId");
                logMap.put(studentId + "_" + courseId, entry);
            }
        }

        // returns the last update OR insert oplog entries for each (sid, cid) pair.
        return new ArrayList<>(logMap.values());
    }
}
