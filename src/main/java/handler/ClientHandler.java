package handler;

import command.Command;
import command.CommandRegistry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final CommandRegistry commandRegistry;
    private final TransactionManager transactionManager;
    private boolean isReplicaConnection = false; // Set to true after PSYNC
    private final Set<String> subscribedChannels = new HashSet<>();

    public ClientHandler(Socket clientSocket, CommandRegistry commandRegistry) {
        this.clientSocket = clientSocket;
        this.commandRegistry = commandRegistry;
        this.transactionManager = new TransactionManager(commandRegistry);
    }
    
    /**
     * Mark this connection as a replica connection (after PSYNC).
     * Commands from replica connections should not send responses.
     */
    public void markAsReplicaConnection() {
        this.isReplicaConnection = true;
    }
    
    public boolean isReplicaConnection() {
        return isReplicaConnection;
    }
    
    public int subscribeChannel(String channel) {
        // Add only if not present; return current count
        subscribedChannels.add(channel);
        return subscribedChannels.size();
    }

    @Override
    public void run() {
        try (clientSocket;
             OutputStream outputStream = clientSocket.getOutputStream();
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(clientSocket.getInputStream()))) {

            String line;
            while ((line = in.readLine()) != null) {
                if (!line.startsWith("*")) continue;

                // Re-position reader to include the line we just read
                String[] parts = parseLine(line, in);
                
                if (parts == null || parts.length == 0 || parts[0] == null) {
                    continue;
                }

                String commandName = parts[0].toUpperCase();
                
                // Check if this is a REPLCONF ACK response from a replica
                if (commandName.equals("REPLCONF") && parts.length >= 3 && parts[1].equalsIgnoreCase("ACK")) {
                    // This is a replica responding to GETACK
                    try {
                        int reportedOffset = Integer.parseInt(parts[2]);
                        // Find the replica connection for this socket and complete its ACK future
                        for (ReplicaConnection r : ReplicationManager.getInstance().getReplicas()) {
                            if (r.getSocket() == clientSocket) {
                                r.completeAck(reportedOffset);
                                System.out.println("Received ACK from replica " + r.getReplicaId() + ": offset=" + reportedOffset);
                                break;
                            }
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid offset in REPLCONF ACK: " + parts[2]);
                    }
                    continue; // Don't process this as a normal command
                }
                
                // Enforce subscribed mode: only allow SUBSCRIBE (and later UNSUBSCRIBE/PING) when subscribed
                if (!subscribedChannels.isEmpty()) {
                    boolean allowed = commandName.equals("SUBSCRIBE");
                    if (!allowed) {
                        String err = String.format(
                            "-ERR Can't execute '%s': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING allowed in this context\r\n",
                            commandName.toLowerCase()
                        );
                        outputStream.write(err.getBytes());
                        outputStream.flush();
                        continue;
                    }
                }

                // Use NullOutputStream for replica connections (master sending commands)
                OutputStream responseStream = isReplicaConnection ? new NullOutputStream() : outputStream;
                
                // Handle transaction commands
                if (commandName.equals("MULTI")) {
                    transactionManager.handleMulti(responseStream);
                    continue;
                }
                
                if (commandName.equals("EXEC")) {
                    transactionManager.handleExec(responseStream);
                    continue;
                }
                
                if (commandName.equals("DISCARD")) {
                    transactionManager.handleDiscard(responseStream);
                    continue;
                }
                
                // If in transaction, queue the command
                if (transactionManager.isInTransaction()) {
                    transactionManager.queueCommand(parts, responseStream);
                    continue;
                }
                
                // Normal command execution
                Command command = commandRegistry.getCommand(commandName);

                if (command != null) {
                    command.execute(parts, responseStream, clientSocket, this);
                } else {
                    responseStream.write("-ERR unknown command\r\n".getBytes());
                    responseStream.flush();
                    System.out.println("Unknown command: " + parts[0]);
                }
            }

            System.out.println("Client disconnected.");
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String[] parseLine(String firstLine, BufferedReader in) throws IOException {
        int arrayCount = Integer.parseInt(firstLine.substring(1));
        String[] parts = new String[arrayCount];

        for (int i = 0; i < parts.length; i++) {
            String lenLine = in.readLine();
            if (lenLine == null || !lenLine.startsWith("$")) break;

            String data = in.readLine();
            parts[i] = data;
        }

        return parts;
    }
}