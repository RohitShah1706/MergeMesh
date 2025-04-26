package com.mergemesh.mongo_server.service;

import com.mergemesh.shared.OplogEntry;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

import org.bson.Document;
import org.springframework.http.ResponseEntity;
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

    public void merge(String server) {
        try {
            Map<String, OplogEntry> gradeMapSelf = new HashMap<>();
            Map<String, OplogEntry> gradeMapRemote = new HashMap<>();

            // Read local log file
            List<OplogEntry> localLog = loggerService.readLogFile();
            for (OplogEntry entry : localLog) {
                if ("graderoster".equals(entry.getTableName())) {
                    String key = entry.getData().get("studentId") + "_" + entry.getData().get("courseId");
                    if ("INSERT".equals(entry.getOperation()) || "UPDATE".equals(entry.getOperation())) {
                        gradeMapSelf.put(key, entry);
                    }
                }
            }

            // Read remote log file
            List<OplogEntry> remoteLog = readRemoteLogFile(server);
            for (OplogEntry entry : remoteLog) {
                if ("graderoster".equals(entry.getTableName())) {
                    String key = entry.getData().get("studentId") + "_" + entry.getData().get("courseId");
                    if ("INSERT".equals(entry.getOperation()) || "UPDATE".equals(entry.getOperation())) {
                        gradeMapRemote.put(key, entry);
                    }
                }
            }

            // Apply changes to the database
            for (Map.Entry<String, OplogEntry> entry : gradeMapRemote.entrySet()) {
                String[] keys = entry.getKey().split("_");
                String studentIdRemote = keys[0];
                String courseIdRemote = keys[1];
                String gradeRemote;
                String operation = entry.getValue().getOperation();
                if (operation.equals("INSERT")) {
                    gradeRemote = entry.getValue().getData().get("grade");
                } else if (operation.equals("UPDATE")) {
                    gradeRemote = entry.getValue().getData().get("newGrade");
                } else {
                    gradeRemote = "Not Available";
                }
                Map<String, String> data = new HashMap<>();
                data.put("studentId", studentIdRemote);
                data.put("courseId", courseIdRemote);

                OplogEntry selfEntry = gradeMapSelf.getOrDefault(entry.getKey(), null);
                if (selfEntry == null) {
                    data.put("grade", gradeRemote);
                    insertGrade(data);
                } else {
                    // If the entry exists in both logs, update it
                    LocalDateTime selfTimestamp = LocalDateTime.parse(selfEntry.getTimestamp());
                    LocalDateTime remoteTimestamp = LocalDateTime.parse(entry.getValue().getTimestamp());
                    if (selfTimestamp.compareTo(remoteTimestamp) < 0) {
                        data.put("newGrade", gradeRemote);
                        updateGrade(data);
                    }
                }

            }
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
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

    private List<OplogEntry> readRemoteLogFile(String server) {
        String URL = server + "/logs";
        ResponseEntity<OplogEntry[]> response = restTemplate.getForEntity(URL, OplogEntry[].class);
        List<OplogEntry> responseBody = Arrays.asList(response.getBody());

        // System.out.println("Grade from hive service: " + responseBody);
        return responseBody;
    }
}
