package com.mergemesh.hive_server;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mergemesh.hive_server.service.HiveService;

import com.mergemesh.shared.OplogEntry;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App
{
    private static final Gson gson = new Gson();

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
            if ("GET".equals(method) && "/".equals(path)) {
                handleGetGrade(exchange);
            } else if ("GET".equals(method) && "/logs".equals(path)) {
                handleGetLogs(exchange);
            } else if("POST".equals(method) && "/".equals(path)) {
                handlePostGrade(exchange);
            } else if ("POST".equals(method) && "/merge".equals(path)) {
                handlePostMerge(exchange);
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
            Map<String, String> req = getReqBody(exchange);

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
            Map<String, String> req = getReqBody(exchange);
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

        private void handleGetLogs(HttpExchange exchange) throws IOException {
            List<OplogEntry> logs = hiveService.getLogValues();
            String jsonResponse = logs.toString();

            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);

            OutputStream os = exchange.getResponseBody();
            os.write(jsonResponse.getBytes());
            os.close();
        }

        private void handlePostMerge(HttpExchange exchange) throws IOException {
            Map<String, String> req = getReqBody(exchange);
            System.out.println(req);
            String server = req.get("server");
            String response;

            try {
                hiveService.merge(server);
                response = "Merge completed successfully.";
                exchange.sendResponseHeaders(200, response.getBytes().length);
            } catch (Exception e) {
                response = "Failed to merge: " + e.getMessage();
                exchange.sendResponseHeaders(500, response.getBytes().length);
            }

            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }

        private static Map<String, String> queryToMap(String query) {
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

        private static Map<String, String> getReqBody(HttpExchange exchange) throws IOException {
            InputStream is = exchange.getRequestBody();
            StringBuilder body = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line);
            }
            reader.close();

            return gson.fromJson(body.toString(), new TypeToken<Map<String, String>>(){}.getType());
        }
    }
}
