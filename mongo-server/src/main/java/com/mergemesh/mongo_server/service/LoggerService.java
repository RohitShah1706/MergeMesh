package com.mergemesh.mongo_server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mergemesh.shared.OplogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class LoggerService {
    private static final Logger logger = LoggerFactory.getLogger(LoggerService.class);

    /**
     * Logs the provided JSON string to the logfile.
     *
     * @param jsonLogEntry JSON representation of an OplogEntry
     */
    public void logToFile(String jsonLogEntry) {
        logger.info(jsonLogEntry);
    }

    public List<OplogEntry> readLogFile() {
        String logFilePath = "./logs/oplog_mongo.log";
        List<OplogEntry> oplogEntries = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Assuming the log entry is in JSON format, parse it into an OplogEntry object
                OplogEntry oplogEntry = new ObjectMapper().readValue(line, OplogEntry.class);
                oplogEntries.add(oplogEntry);
            }
        } catch (IOException e) {
            logger.error("Error reading log file", e);
        }
        return oplogEntries;
    }
}
