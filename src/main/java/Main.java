import java.io.*;
import java.net.*;
import java.util.*;

public class Main {
    
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running
        // tests.
        System.err.println("Logs from your program will appear here!");
        
        try (ServerSocket serverSocket = new ServerSocket(6379)) {
            System.err.println("Redis server listening on port 6379");
            
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.err.println("Client connected: " + clientSocket.getRemoteSocketAddress());
                
                // Handle client in a separate thread
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Error starting server: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void handleClient(Socket clientSocket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                System.err.println("Received: " + line);
                
                // Parse RESP protocol and handle commands
                String response = processCommand(line, reader);
                if (response != null) {
                    writer.println(response);
                    System.err.println("Sent: " + response);
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
    
    private static String processCommand(String line, BufferedReader reader) throws IOException {
        // Simple RESP parser for basic commands
        if (line.startsWith("*")) {
            // Array of bulk strings
            int arrayLength = Integer.parseInt(line.substring(1));
            List<String> command = new ArrayList<>();
            
            for (int i = 0; i < arrayLength; i++) {
                // Read bulk string length
                String bulkStringLength = reader.readLine();
                if (bulkStringLength.startsWith("$")) {
                    // Read the actual string
                    String value = reader.readLine();
                    command.add(value);
                }
            }
            
            if (command.size() > 0) {
                String cmd = command.get(0).toUpperCase();
                
                switch (cmd) {
                    case "ECHO":
                        if (command.size() >= 2) {
                            return "$" + command.get(1).length() + "\r\n" + command.get(1) + "\r\n";
                        }
                        break;
                    case "PING":
                        return "+PONG\r\n";
                    default:
                        return "-ERR unknown command '" + cmd + "'\r\n";
                }
            }
        }
        
        return null;
    }
}