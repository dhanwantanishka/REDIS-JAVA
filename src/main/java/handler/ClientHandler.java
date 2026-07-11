package handler;

import command.Command;
import command.CommandRegistry;
import protocol.RESPParser;

import java.io.*;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Handles a single client connection in its own thread.
 * Parses RESP commands, routes them to Command implementations,
 * and supports transactions, pub/sub, and replication.
 */
public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final CommandRegistry commandRegistry;
    private final TransactionManager transactionManager;
    private final Set<String> subscribedChannels = Collections.synchronizedSet(new HashSet<>());
    private volatile boolean isReplicaConnection = false;

    public ClientHandler(Socket clientSocket, CommandRegistry commandRegistry) {
        this.clientSocket = clientSocket;
        this.commandRegistry = commandRegistry;
        this.transactionManager = new TransactionManager(commandRegistry);
    }

    @Override
    public void run() {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            OutputStream out = clientSocket.getOutputStream()
        ) {
            while (!clientSocket.isClosed()) {
                String[] parts = RESPParser.parseCommand(in);
                if (parts == null) {
                    break; // Client disconnected
                }

                String commandName = parts[0].toUpperCase();

                // Handle transaction commands specially
                switch (commandName) {
                    case "MULTI":
                        transactionManager.handleMulti(out);
                        continue;
                    case "EXEC":
                        transactionManager.handleExec(out);
                        continue;
                    case "DISCARD":
                        transactionManager.handleDiscard(out);
                        continue;
                }

                // If we're in a transaction, queue the command instead of executing it
                if (transactionManager.isInTransaction()) {
                    transactionManager.queueCommand(parts, out);
                    continue;
                }

                // Look up the command
                Command command = commandRegistry.getCommand(commandName);
                if (command == null) {
                    out.write(("-ERR unknown command '" + parts[0] + "'\r\n").getBytes());
                    out.flush();
                    continue;
                }

                // Execute the command with appropriate context
                try {
                    command.execute(parts, out, clientSocket, this);
                } catch (Exception e) {
                    System.err.println("Error executing " + commandName + ": " + e.getMessage());
                    try {
                        out.write(("-ERR " + e.getMessage() + "\r\n").getBytes());
                        out.flush();
                    } catch (IOException ignored) {}
                }

                // After executing, check if this became a replica connection
                // If so, we need to stay in this loop to keep reading ACK responses
                if (isReplicaConnection) {
                    handleReplicaMode(in, out);
                    break; // handleReplicaMode runs until connection closes
                }
            }
        } catch (IOException e) {
            // Client disconnected or I/O error
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
        } catch (Exception e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            // Clean up pub/sub subscriptions
            for (String channel : subscribedChannels) {
                PubSubManager.getInstance().unsubscribe(channel, null);
            }
            try {
                clientSocket.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Once a connection is marked as a replica (after PSYNC), we enter this mode
     * to read ACK responses from the replica.
     */
    private void handleReplicaMode(BufferedReader in, OutputStream out) {
        try {
            while (!clientSocket.isClosed()) {
                String[] parts = RESPParser.parseCommand(in);
                if (parts == null) {
                    break; // Replica disconnected
                }

                String commandName = parts[0].toUpperCase();

                // The primary thing we expect from a replica is REPLCONF ACK <offset>
                if (commandName.equals("REPLCONF") && parts.length >= 3 
                        && parts[1].equalsIgnoreCase("ACK")) {
                    try {
                        int reportedOffset = Integer.parseInt(parts[2]);
                        // Find this replica connection and complete its ACK future
                        for (ReplicaConnection replica : ReplicationManager.getInstance().getReplicas()) {
                            if (replica.getSocket() == clientSocket) {
                                replica.completeAck(reportedOffset);
                                break;
                            }
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid ACK offset: " + parts[2]);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Replica connection error: " + e.getMessage());
        }
    }

    /**
     * Mark this connection as a replica connection (called after PSYNC).
     * The handler will switch to replica mode after the current command completes.
     */
    public void markAsReplicaConnection() {
        this.isReplicaConnection = true;
    }

    /**
     * Subscribe to a channel and return the updated subscription count.
     */
    public int subscribeChannel(String channel) {
        subscribedChannels.add(channel);
        return subscribedChannels.size();
    }

    /**
     * Unsubscribe from a channel and return the updated subscription count.
     */
    public int unsubscribeChannel(String channel) {
        subscribedChannels.remove(channel);
        return subscribedChannels.size();
    }

    /**
     * Return an unmodifiable snapshot of subscribed channels.
     */
    public Set<String> getChannels() {
        synchronized (subscribedChannels) {
            return Collections.unmodifiableSet(new HashSet<>(subscribedChannels));
        }
    }
}
