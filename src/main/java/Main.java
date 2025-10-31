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
            String rawOpt = args[i];
            String opt = rawOpt.toLowerCase();
            switch (opt) {
                case "--port":
                case "-p":
                case "port":
                    if (i + 1 < args.length) {
                        port = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--replicaof":
                case "-replicaof":
                case "replicaof":
                {
                    // Forms supported:
                    //   --replicaof HOST PORT
                    //   --replicaof HOST:PORT
                    //   --replicaof=HOST PORT
                    //   --replicaof=HOST:PORT
                    String value = null;
                    if (rawOpt.contains("=")) {
                        value = rawOpt.substring(rawOpt.indexOf('=') + 1);
                    } else if (i + 1 < args.length) {
                        // Peek next token(s)
                        String next = args[++i];
                        // If the next token contains a colon, it's HOST:PORT; otherwise, expect another token as PORT
                        if (next.contains(":")) {
                            value = next;
                        } else {
                            if (i + 1 < args.length) {
                                value = next + " " + args[++i];
                            } else {
                                // Not enough args; ignore
                                break;
                            }
                        }
                    }
                    if (value != null) {
                        String hostCandidate;
                        String portCandidate;
                        if (value.contains(":")) {
                            String[] hp = value.split(":", 2);
                            hostCandidate = hp[0];
                            portCandidate = hp[1];
                        } else {
                            String[] hp = value.trim().split("\\s+", 2);
                            if (hp.length < 2) break;
                            hostCandidate = hp[0];
                            portCandidate = hp[1];
                        }
                        try {
                            masterHost = hostCandidate;
                            masterPort = Integer.parseInt(portCandidate);
                            serverRole = "slave";
                        } catch (NumberFormatException ignored) {
                        }
                    }
                    break;
                }
                case "--dir":
                case "-dir":
                case "dir":
                    if (i + 1 < args.length) {
                        rdbDir = args[++i];
                    }
                    break;
                case "--dbfilename":
                case "-dbfilename":
                case "dbfilename":
                    if (i + 1 < args.length) {
                        rdbFilename = args[++i];
                    }
                    break;
            }
        }
        
        // Extra robust scan for replica config if not detected above
        if ("master".equals(serverRole)) {
            for (int i = 0; i < args.length; i++) {
                String token = args[i];
                if (token == null) continue;
                String normalized = token.toLowerCase()
                        .replace('\u2013', '-') // en dash
                        .replace('\u2014', '-') // em dash
                        .replace('\u2212', '-') // minus sign
                        .trim();
                String bare = normalized.replaceFirst("^-+", "");
                if (bare.startsWith("replicaof")) {
                    String value = null;
                    if (token.contains("=")) {
                        value = token.substring(token.indexOf('=') + 1);
                    } else {
                        // Try HOST:PORT in next token or HOST then PORT
                        if (i + 1 < args.length) {
                            String next = args[i + 1];
                            if (next != null && next.contains(":")) {
                                value = next;
                            } else if (i + 2 < args.length) {
                                value = next + " " + args[i + 2];
                            }
                        }
                    }
                    if (value != null) {
                        String hostCandidate;
                        String portCandidate;
                        if (value.contains(":")) {
                            String[] hp = value.split(":", 2);
                            hostCandidate = hp[0];
                            portCandidate = hp[1];
                        } else {
                            String[] hp = value.trim().split("\\s+", 2);
                            if (hp.length < 2) break;
                            hostCandidate = hp[0];
                            portCandidate = hp[1];
                        }
                        try {
                            masterHost = hostCandidate;
                            masterPort = Integer.parseInt(portCandidate);
                            serverRole = "slave";
                        } catch (NumberFormatException ignored) {}
                    }
                    break;
                }
            }
        }

        // Environment variable fallback for replica configuration
        // This helps in environments that provide replica configuration via env vars
        if ("master".equals(serverRole)) {
            String envMasterHost =
                System.getenv("REPLICAOF_HOST") != null ? System.getenv("REPLICAOF_HOST") :
                System.getenv("REPLICA_OF_HOST") != null ? System.getenv("REPLICA_OF_HOST") :
                System.getenv("MASTER_HOST");
            String envMasterPort =
                System.getenv("REPLICAOF_PORT") != null ? System.getenv("REPLICAOF_PORT") :
                System.getenv("REPLICA_OF_PORT") != null ? System.getenv("REPLICA_OF_PORT") :
                System.getenv("MASTER_PORT");
            if (envMasterHost != null && envMasterPort != null) {
                try {
                    masterHost = envMasterHost;
                    masterPort = Integer.parseInt(envMasterPort);
                    serverRole = "slave";
                } catch (NumberFormatException ignored) {
                    // If port isn't a valid number, ignore env fallback
                }
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
            connectToMaster(masterHost, masterPort, port, commandRegistry);
        }
        
        // Start the server
        startServer(port, commandRegistry);
    }
    
    private static void connectToMaster(String masterHost, int masterPort, int localPort, CommandRegistry commandRegistry) {
        try {
            System.out.println("Connecting to master " + masterHost + ":" + masterPort);
            
            // Perform replica handshake
            ReplicaHandshake handshake = new ReplicaHandshake(masterHost, masterPort, localPort, commandRegistry);
            
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
