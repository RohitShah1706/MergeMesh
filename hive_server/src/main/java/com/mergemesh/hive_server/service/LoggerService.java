package com.mergemesh.hive_server.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
}
