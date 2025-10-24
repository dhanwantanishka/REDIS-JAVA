import handler.ClientHandler;
import handler.ReplicationManager;
import handler.ReplicaHandshake;
import command.CommandRegistry;
import storage.RedisStore;
import storage.RDBReader;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int DEFAULT_PORT = 6379;
    private static final String DEFAULT_RDB_DIR = ".";
    private static final String DEFAULT_RDB_FILENAME = "dump.rdb";
    
    public static void main(String[] args) {
        // Parse command line arguments
        String serverRole = "master";
        String masterHost = null;
        int masterPort = 6379;
        int port = DEFAULT_PORT;
        String rdbDir = DEFAULT_RDB_DIR;
        String rdbFilename = DEFAULT_RDB_FILENAME;
        String masterReplId = null;
        Integer masterReplOffset = null;
        
        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 < args.length) {
                        port = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--replicaof":
                    if (i + 2 < args.length) {
                        serverRole = "slave";
                        masterHost = args[++i];
                        masterPort = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--dir":
                    if (i + 1 < args.length) {
                        rdbDir = args[++i];
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        rdbFilename = args[++i];
                    }
                    break;
            }
        }
        
        System.out.println("Starting Redis server...");
        System.out.println("Port: " + port);
        System.out.println("Role: " + serverRole);
        if (serverRole.equals("slave")) {
            System.out.println("Master: " + masterHost + ":" + masterPort);
        }
        System.out.println("RDB Dir: " + rdbDir);
        System.out.println("RDB Filename: " + rdbFilename);
        
        // Initialize components
        RedisStore store = new RedisStore();
        ReplicationManager replicationManager = ReplicationManager.getInstance();
        replicationManager.setServerRole(serverRole);
        
        // Load RDB file if it exists
        boolean rdbLoaded = RDBReader.loadFromFile(rdbDir, rdbFilename, store);
        if (rdbLoaded) {
            System.out.println("RDB file loaded successfully");
        } else {
            System.out.println("No RDB file found or error loading");
        }
        
        // Initialize command registry
        CommandRegistry commandRegistry = new CommandRegistry(
            store, serverRole, masterReplId, masterReplOffset, rdbDir, rdbFilename
        );
        
        // If this is a replica, connect to master first
        if (serverRole.equals("slave") && masterHost != null) {
            connectToMaster(masterHost, masterPort, commandRegistry);
        }
        
        // Start the server
        startServer(port, commandRegistry);
    }
    
    private static void connectToMaster(String masterHost, int masterPort, CommandRegistry commandRegistry) {
        try {
            System.out.println("Connecting to master " + masterHost + ":" + masterPort);
            
            // Perform replica handshake
            ReplicaHandshake handshake = new ReplicaHandshake(masterHost, masterPort, DEFAULT_PORT, commandRegistry);
            
            // Start handshake in a separate thread since it blocks
            Thread replicaThread = new Thread(() -> {
                try {
                    handshake.performHandshake();
                    System.out.println("Successfully connected to master as replica");
                } catch (Exception e) {
                    System.err.println("Error in replica connection: " + e.getMessage());
                }
            });
            replicaThread.setDaemon(true);
            replicaThread.start();
            
        } catch (Exception e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
        }
    }
    
    private static void startServer(int port, CommandRegistry commandRegistry) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Redis server listening on port " + port);
            
            // Create thread pool for handling client connections
            ExecutorService executor = Executors.newCachedThreadPool();
            
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("New client connected: " + clientSocket.getRemoteSocketAddress());
                    
                    // Handle each client in a separate thread
                    ClientHandler clientHandler = new ClientHandler(clientSocket, commandRegistry);
                    executor.submit(clientHandler);
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to start server on port " + port + ": " + e.getMessage());
            System.exit(1);
        }
    }
}
