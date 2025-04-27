package com.mergemesh.shared;

import java.util.List;
import java.util.Map;

public interface Server {
    String getGrade(String studentId, String courseId) throws Exception;

    void insertGrade(Map<String, String> req) throws Exception;

    void updateGrade(Map<String, String> req) throws Exception;

    List<OplogEntry> getLogValues();

    default void merge(String server) {
        try {
            Map<String, OplogEntry> gradeMapSelf = new java.util.HashMap<>();
            Map<String, OplogEntry> gradeMapRemote = new java.util.HashMap<>();

            List<OplogEntry> localLog = getLogValues();
            for (OplogEntry entry : localLog) {
                if ("graderoster".equals(entry.getTableName())) {
                    String key = entry.getData().get("studentId") + "_" + entry.getData().get("courseId");
                    if ("INSERT".equals(entry.getOperation()) || "UPDATE".equals(entry.getOperation())) {
                        gradeMapSelf.put(key, entry);
                    }
                }
            }

            List<OplogEntry> remoteLog = readRemoteLogFile(server);
            for (OplogEntry entry : remoteLog) {
                if ("graderoster".equals(entry.getTableName())) {
                    String key = entry.getData().get("studentId") + "_" + entry.getData().get("courseId");
                    if ("INSERT".equals(entry.getOperation()) || "UPDATE".equals(entry.getOperation())) {
                        gradeMapRemote.put(key, entry);
                    }
                }
            }

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
                Map<String, String> data = new java.util.HashMap<>();
                data.put("studentId", studentIdRemote);
                data.put("courseId", courseIdRemote);

                OplogEntry selfEntry = gradeMapSelf.getOrDefault(entry.getKey(), null);
                if (selfEntry == null) {
                    data.put("grade", gradeRemote);
                    data.put("timestamp",entry.getValue().getTimestamp());
                    insertGrade(data);
                } else {
                    java.time.LocalDateTime selfTimestamp = java.time.LocalDateTime.parse(selfEntry.getTimestamp());
                    java.time.LocalDateTime remoteTimestamp = java.time.LocalDateTime
                            .parse(entry.getValue().getTimestamp());
                    if (selfTimestamp.compareTo(remoteTimestamp) < 0) {
                        data.put("newGrade", gradeRemote);
                        data.put("timestamp",entry.getValue().getTimestamp());
                        updateGrade(data);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Exception during merge: " + e.getMessage());
        }
    }

    List<OplogEntry> readRemoteLogFile(String server);
}
