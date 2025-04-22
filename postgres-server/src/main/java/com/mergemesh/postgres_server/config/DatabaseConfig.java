package com.mergemesh.postgres_server.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Configuration
public class DatabaseConfig {
    @Bean
    public Connection postgresConnection() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/mergemesh";
        return DriverManager.getConnection(url, "myuser", "mypassword");
    }
}
