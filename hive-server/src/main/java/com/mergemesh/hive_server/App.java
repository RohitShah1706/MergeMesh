package com.mergemesh.hive_server;

import com.mergemesh.hive_server.service.HiveService;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class App
{
    public static void main(String[] args ) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8081), 0);
        server.createContext("/", new MarksHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
        System.out.println("Server started at http://localhost:8081/");
    }

    static class MarksHandler implements HttpHandler {
        private final HiveService hiveService = new HiveService();
        
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();

            System.out.println("Received request: " + method + " " + path);
            if ("GET".equals(method)) {
                handleGetGrade(exchange);
            } else if("POST".equals(method)) {
                handlePostGrade(exchange);
            } else if ("PUT".equals(method)) {
                handleUpdateGrade(exchange);
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }

        private void handleGetGrade(HttpExchange exchange) throws IOException {
            Map<String, String> params = queryToMap(exchange.getRequestURI().getQuery());
            String studentId = params.get("studentId");
            String courseId = params.get("courseId");
            String response;
            try {
                response = hiveService.getGrade(studentId, courseId);
                exchange.sendResponseHeaders(200, response.getBytes().length);
            } catch (Exception e) {
                response = "Error: " + e.getMessage();
                exchange.sendResponseHeaders(500, response.getBytes().length);
            }

            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }

        private void handlePostGrade(HttpExchange exchange) throws IOException {
            InputStream is = exchange.getRequestBody();
            StringBuilder body =  new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line);
            }
            reader.close();

            Map<String, String> req = parseJson(body.toString());

            String response;
            try {
                hiveService.insertGrade(req);
                response = "Grades inserted successfully.";
                exchange.sendResponseHeaders(200, response.getBytes().length);
            } catch (Exception e) {
                response = "Failed to insert grades: " + e.getMessage();
                exchange.sendResponseHeaders(500, response.getBytes().length);
            }

            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }

        private void handleUpdateGrade(HttpExchange exchange) throws IOException {
            InputStream is = exchange.getRequestBody();
            StringBuilder body = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line);
            }
            reader.close();

            Map<String, String> req = parseJson(body.toString());

            String response;
            try {
                hiveService.updateGrade(req);
                response = "Grades updated successfully.";
                exchange.sendResponseHeaders(200, response.getBytes().length);
            } catch (Exception e) {
                response = "Failed to update grades: " + e.getMessage();
                exchange.sendResponseHeaders(500, response.getBytes().length);
            }

            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }

        private Map<String, String> queryToMap(String query) {
            Map<String, String> result = new HashMap<>();
            if (query != null) {
                for (String param : query.split("&")) {
                    String[] entry = param.split("=");
                    if (entry.length > 1) {
                        result.put(entry[0], entry[1]);
                    }
                }
            }
            return result;
        }

        // Very basic JSON parser (assumes flat JSON with string values)
        private Map<String, String> parseJson(String json) {
            Map<String, String> map = new HashMap<>();
            json = json.trim().replaceAll("[{}\"]", "");
            for (String pair : json.split(",")) {
                String[] kv = pair.split(":");
                if (kv.length == 2) {
                    map.put(kv[0].trim(), kv[1].trim());
                }
            }
            return map;
        }
    }
}
