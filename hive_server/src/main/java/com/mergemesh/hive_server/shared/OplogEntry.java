package com.mergemesh.hive_server.shared;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;

@Getter
@Setter
public class OplogEntry {

    /**
     * Operation type: "QUERY", "UPDATE", or "MERGE"
     */
    private String operation;

    /**
     * Name of the table/collection affected.
     * For MERGE, this can represent the target table.
     */
    private String tableName;

    /**
     * Map of key-value updates, e.g., { "obtainedMarks": "92" }
     * For MERGE, this can store metadata like sourceSystem
     */
    private Map<String, String> data;

    /**
     * Timestamp of when the operation was performed.
     */
    private String timestamp;

    public OplogEntry() {}

    public OplogEntry(String operation, String tableName, Map<String, String> data, String timestamp) {
        this.operation = operation;
        this.tableName = tableName;
        this.data = data;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "{\"error\": \"Failed to serialize OplogEntry\"}" + e.getMessage();
        }
    }
}
