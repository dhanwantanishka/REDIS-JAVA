import command.CommandRegistry;
import handler.ClientHandler;
import handler.ReplicaHandshake;
import handler.ReplicationManager;
import storage.RDBReader;
import storage.RedisStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  private static final RedisStore store = new RedisStore();
  private static final int DEFAULT_PORT = 6379;
  private static final String DEFAULT_RDB_DIR = "/tmp/redis-files";
  private static final String DEFAULT_RDB_FILENAME = "dump.rdb";
  
  private static String serverRole = "master"; // Default role
  private static String masterReplId = null;  // Will be set based on role
  private static Integer masterReplOffset = 0;
  private static CommandRegistry commandRegistry;
  private static String masterHost = null;
  private static int masterPort = 0;
  private static int serverPort = DEFAULT_PORT;
  private static String rdbDir = DEFAULT_RDB_DIR;
  private static String rdbFilename = DEFAULT_RDB_FILENAME;

  public static void main(String[] args) {
    // Parse command line arguments
    parseArguments(args);
    
    // Generate replication ID if this is a master
    if (serverRole.equals("master")) {
      masterReplId = generateReplId();
      System.out.println("Generated replication ID: " + masterReplId);
    }
    
    // Initialize command registry with server role
    commandRegistry = new CommandRegistry(store, serverRole, masterReplId, masterReplOffset, rdbDir, rdbFilename);
    
    // Load data from RDB file if it exists
    RDBReader.loadFromFile(rdbDir, rdbFilename, store);
    
    // Set server role in ReplicationManager
    ReplicationManager.getInstance().setServerRole(serverRole);
    
    // If replica, perform handshake with master
    if (serverRole.equals("slave") && masterHost != null) {
      System.out.println(String.format("Configured as replica of %s:%d", masterHost, masterPort));
      ReplicaHandshake handshake = new ReplicaHandshake(masterHost, masterPort, serverPort, commandRegistry);
      new Thread(() -> handshake.performHandshake()).start();
    }
    
    // Start server
    startServer(serverPort);
  }
  
  private static void parseArguments(String[] args) {
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--port":
        case "-p":
          if (i + 1 >= args.length) {
            exitWithError("--port requires a port number");
          }
          try {
            serverPort = Integer.parseInt(args[i + 1]);
            i++; // skip the next argument since we've used it
          } catch (NumberFormatException e) {
            exitWithError("Invalid port number '" + args[i + 1] + "'");
          }
          break;
          
        case "--help":
        case "-h":
          printUsage();
          System.exit(0);
          break;
          
        case "--replicaof":
          if (i + 1 >= args.length) {
            exitWithError("--replicaof requires host and port");
          }
          String[] master = args[i + 1].split("\\s+");
          if (master.length <= 1) {
            exitWithError("Invalid master format. Expected: 'host port'");
          }
          masterHost = master[0];
          try {
            masterPort = Integer.parseInt(master[1]);
            serverRole = "slave";
            i++; // skip the next argument since we've used it
          } catch (NumberFormatException e) {
            exitWithError("Invalid master port number '" + master[1] + "'");
          }
          break;
          
        case "--dir":
          if (i + 1 >= args.length) {
            exitWithError("--dir requires a directory path");
          }
          rdbDir = args[i + 1];
          i++;
          break;
          
        case "--dbfilename":
          if (i + 1 >= args.length) {
            exitWithError("--dbfilename requires a filename");
          }
          rdbFilename = args[i + 1];
          i++;
          break;
          
        default:
          exitWithError("Unknown argument '" + args[i] + "'");
      }
    }
  }
  
  private static void startServer(int port) {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      // SO_REUSEADDR ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      
      System.out.println(String.format("Server started successfully on port %d", port));
      System.out.println("Waiting for connections...");

      while (true) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("New client connected");
        new Thread(new ClientHandler(clientSocket, commandRegistry)).start();
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
      System.exit(-1);
    }
  }
  
  private static void exitWithError(String message) {
    System.err.println("Error: " + message);
    printUsage();
    System.exit(1);
  }

  private static void printUsage() {
    System.out.println("Redis Server - Java Implementation");
    System.out.println();
    System.out.println("Usage: java Main [OPTIONS]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --port, -p <port>              Set the port number (default: 6379)");
    System.out.println("  --replicaof <host> <port>      Run as replica of specified master");
    System.out.println("  --dir <directory>              Set the RDB directory (default: /tmp/redis-files)");
    System.out.println("  --dbfilename <filename>        Set the RDB filename (default: dump.rdb)");
    System.out.println("  --help, -h                     Show this help message");
    System.out.println();
    System.out.println("Examples:");
    System.out.println("  java Main");
    System.out.println("  java Main --port 6380");
    System.out.println("  java Main -p 7000");
    System.out.println("  java Main --port 6380 --replicaof localhost 6379");
    System.out.println("  java Main --dir /var/redis --dbfilename dump.rdb");
  }
  
  private static String generateReplId() {
    // Generate a random 40-character hexadecimal string
    // In a real implementation, this should be persistent across restarts
    StringBuilder sb = new StringBuilder(40);
    String hex = "0123456789abcdef";
    java.util.Random random = new java.util.Random();
    for (int i = 0; i < 40; i++) {
      sb.append(hex.charAt(random.nextInt(16)));
    }
    return sb.toString();
  }

}