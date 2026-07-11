package handler;

import command.Command;
import command.CommandRegistry;
import handler.ReplicationManager;
import storage.RedisStore;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Lightweight HTTP server for the Redis dashboard.
 * Provides REST API endpoints and serves static frontend files.
 */
public class RedisHttpServer {
    private final HttpServer httpServer;
    private final RedisStore store;
    private final CommandRegistry commandRegistry;
    private final long startTime;

    public RedisHttpServer(int port, RedisStore store, CommandRegistry commandRegistry) throws IOException {
        this.store = store;
        this.commandRegistry = commandRegistry;
        this.startTime = System.currentTimeMillis();

        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.setExecutor(Executors.newFixedThreadPool(4));

        // API endpoints
        httpServer.createContext("/api/ping", this::handlePing);
        httpServer.createContext("/api/info", this::handleInfo);
        httpServer.createContext("/api/keys", this::handleKeys);
        httpServer.createContext("/api/command", this::handleCommand);
        httpServer.createContext("/api/stats", this::handleStats);

        // Static file serving
        httpServer.createContext("/", this::handleStaticFile);
    }

    public void start() {
        httpServer.start();
        System.out.println("Dashboard HTTP server started on port " + httpServer.getAddress().getPort());
    }

    public void stop() {
        httpServer.stop(0);
    }

    private void handlePing(HttpExchange exchange) throws IOException {
        sendJson(exchange, 200, "{\"status\":\"ok\",\"message\":\"PONG\"}");
    }

    private void handleInfo(HttpExchange exchange) throws IOException {
        String role = commandRegistry.getServerRole();
        int replicaCount = ReplicationManager.getInstance().getReplicaCount();
        int keyCount = store.getAllKeys().size();
        long uptimeSeconds = (System.currentTimeMillis() - startTime) / 1000;

        String json = String.format(
            "{\"role\":\"%s\",\"replicas\":%d,\"keys\":%d,\"uptime\":%d,\"commands\":%s}",
            escapeJson(role), replicaCount, keyCount, uptimeSeconds,
            toJsonArray(commandRegistry.getCommandNames().stream()
                .sorted()
                .map(s -> "\"" + escapeJson(s) + "\"")
                .collect(Collectors.toList()))
        );
        sendJson(exchange, 200, json);
    }

    private void handleKeys(HttpExchange exchange) throws IOException {
        List<String> keys = store.getAllKeys();
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String type = getKeyType(key);
            String value = getKeyValue(key, type);
            if (i > 0) json.append(",");
            json.append(String.format("{\"key\":\"%s\",\"type\":\"%s\",\"value\":\"%s\"}",
                escapeJson(key), escapeJson(type), escapeJson(value)));
        }
        json.append("]");
        sendJson(exchange, 200, json.toString());
    }

    private void handleCommand(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        // Read the command from request body
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        // Expect JSON: {"command": "PING"} or {"command": "SET key value"}
        String commandStr = extractJsonValue(body, "command");
        if (commandStr == null || commandStr.trim().isEmpty()) {
            sendJson(exchange, 400, "{\"error\":\"Missing 'command' field\"}");
            return;
        }

        // Parse the command string into parts
        String[] parts = commandStr.trim().split("\\s+");
        if (parts.length == 0) {
            sendJson(exchange, 400, "{\"error\":\"Empty command\"}");
            return;
        }

        String commandName = parts[0].toUpperCase();

        // Handle MULTI/EXEC/DISCARD specially
        if (commandName.equals("MULTI") || commandName.equals("EXEC") || commandName.equals("DISCARD")) {
            sendJson(exchange, 200, "{\"result\":\"" + commandName + " not supported via HTTP dashboard\"}");
            return;
        }

        Command command = commandRegistry.getCommand(commandName);
        if (command == null) {
            sendJson(exchange, 400, "{\"error\":\"Unknown command: " + escapeJson(commandName) + "\"}");
            return;
        }

        // Execute the command and capture output
        ByteArrayOutputStream capturedOutput = new ByteArrayOutputStream();
        try {
            command.execute(parts, capturedOutput);
            String result = capturedOutput.toString(StandardCharsets.UTF_8);
            sendJson(exchange, 200, "{\"result\":\"" + escapeJson(result.trim()) + "\"}");
        } catch (Exception e) {
            sendJson(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private void handleStats(HttpExchange exchange) throws IOException {
        long uptimeSeconds = (System.currentTimeMillis() - startTime) / 1000;
        int keyCount = store.getAllKeys().size();
        int replicaCount = ReplicationManager.getInstance().getReplicaCount();
        long maxMemory = Runtime.getRuntime().maxMemory();
        long totalMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        long usedMemory = totalMemory - freeMemory;

        String json = String.format(
            "{\"uptime\":%d,\"keys\":%d,\"replicas\":%d," +
            "\"memory\":{\"used\":%d,\"total\":%d,\"max\":%d}," +
            "\"role\":\"%s\",\"javaVersion\":\"%s\"}",
            uptimeSeconds, keyCount, replicaCount,
            usedMemory, totalMemory, maxMemory,
            escapeJson(commandRegistry.getServerRole()),
            escapeJson(System.getProperty("java.version", "unknown"))
        );
        sendJson(exchange, 200, json);
    }

    private void handleStaticFile(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/")) path = "/index.html";

        // Try to load from classpath (resources/static/)
        String resourcePath = "/static" + path;
        InputStream is = getClass().getResourceAsStream(resourcePath);

        if (is == null) {
            // Send 404
            String notFound = "<html><body><h1>404 Not Found</h1></body></html>";
            exchange.getResponseHeaders().set("Content-Type", "text/html");
            byte[] bytes = notFound.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(404, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().close();
            return;
        }

        byte[] fileBytes = is.readAllBytes();
        is.close();

        // Set content type based on extension
        String contentType = getContentType(path);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(200, fileBytes.length);
        exchange.getResponseBody().write(fileBytes);
        exchange.getResponseBody().close();
    }

    // --- Utility Methods ---

    private String getKeyType(String key) {
        if (store.containsString(key)) return "string";
        if (store.containsList(key)) return "list";
        if (store.containsStream(key)) return "stream";
        return "none";
    }

    private String getKeyValue(String key, String type) {
        switch (type) {
            case "string":
                String val = store.get(key);
                return val != null ? val : "(nil)";
            case "list":
                var list = store.getList(key);
                if (list == null) return "(empty list)";
                return list.stream().limit(5).collect(Collectors.joining(", "))
                    + (list.size() > 5 ? " ..." : "");
            case "stream":
                var stream = store.getStream(key);
                if (stream == null) return "(empty stream)";
                return stream.size() + " entries";
            default:
                return "(nil)";
        }
    }

    private void sendJson(HttpExchange exchange, int statusCode, String json) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");

        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.getResponseBody().close();
    }

    private String getContentType(String path) {
        if (path.endsWith(".html")) return "text/html; charset=utf-8";
        if (path.endsWith(".css")) return "text/css; charset=utf-8";
        if (path.endsWith(".js")) return "application/javascript; charset=utf-8";
        if (path.endsWith(".json")) return "application/json; charset=utf-8";
        if (path.endsWith(".png")) return "image/png";
        if (path.endsWith(".svg")) return "image/svg+xml";
        if (path.endsWith(".ico")) return "image/x-icon";
        return "text/plain";
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private String extractJsonValue(String json, String key) {
        // Simple JSON value extraction (no external lib dependency)
        String searchKey = "\"" + key + "\"";
        int keyIndex = json.indexOf(searchKey);
        if (keyIndex < 0) return null;
        int colonIndex = json.indexOf(':', keyIndex + searchKey.length());
        if (colonIndex < 0) return null;
        int valueStart = json.indexOf('"', colonIndex + 1);
        if (valueStart < 0) return null;
        int valueEnd = json.indexOf('"', valueStart + 1);
        // Handle escaped quotes
        while (valueEnd > 0 && json.charAt(valueEnd - 1) == '\\') {
            valueEnd = json.indexOf('"', valueEnd + 1);
        }
        if (valueEnd < 0) return null;
        return json.substring(valueStart + 1, valueEnd);
    }

    private String toJsonArray(List<String> items) {
        return "[" + String.join(",", items) + "]";
    }
}
