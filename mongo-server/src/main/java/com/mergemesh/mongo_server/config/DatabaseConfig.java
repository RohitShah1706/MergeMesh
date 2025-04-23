package com.mergemesh.mongo_server.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabaseConfig {
    @Bean
    public MongoClient mongoClient() {
        String uri = "mongodb://myuser:mypassword@localhost:27017/?authSource=admin";
        return MongoClients.create(uri);
    }
}
