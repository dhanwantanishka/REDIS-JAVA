package handler;

import command.Command;
import command.CommandRegistry;

import protocol.RESPWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final CommandRegistry commandRegistry;
    private final TransactionManager transactionManager;
    private boolean isReplicaConnection = false; // Set to true after PSYNC

    // Use synchronized set to avoid concurrency issues if needed; else synchronized explicitly
    private final Set<String> subscribedChannels = Collections.synchronizedSet(new HashSet<>());

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

    // Subscribe channel; only adds if not present; returns total count
    public int subscribeChannel(String channel) {
        subscribedChannels.add(channel);
        return subscribedChannels.size();
    }

    // Unsubscribe channel; removes if present; returns total count after removal
    public int unsubscribeChannel(String channel) {
        subscribedChannels.remove(channel);
        return subscribedChannels.size();
    }

    // Returns a thread-safe unmodifiable view of subscribed channels
    public Set<String> getChannels() {
        synchronized (subscribedChannels) {
            return Collections.unmodifiableSet(new HashSet<>(subscribedChannels));
        }
    }

    @Override
    public void run() {
        try (
            clientSocket;
            OutputStream outputStream = clientSocket.getOutputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
        ) {

            String line;
            while ((line = in.readLine()) != null) {
                if (!line.startsWith("*")) continue;

                String[] parts = parseLine(line, in);
                if (parts == null || parts.length == 0 || parts[0] == null) {
                    continue;
                }

                String commandName = parts[0].toUpperCase();

                // Handle REPLCONF ACK from replicas
                if (commandName.equals("REPLCONF") && parts.length >= 3 && parts[1].equalsIgnoreCase("ACK")) {
                    try {
                        int reportedOffset = Integer.parseInt(parts[2]);
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
                    continue; // Skip normal processing
                }

                // Enforce subscribed mode: allow only SUBSCRIBE, UNSUBSCRIBE, PING commands
                if (!subscribedChannels.isEmpty()) {
                    if (commandName.equals("PING")) {
                        String message = parts.length >= 2 ? parts[1] : "";
                        RESPWriter.writeArrayHeader(outputStream, 2);
                        RESPWriter.writeBulkString(outputStream, "pong");
                        RESPWriter.writeBulkString(outputStream, message);
                        continue;
                    }
                    if (!commandName.equals("SUBSCRIBE") && !commandName.equals("UNSUBSCRIBE")) {
                        String err = String.format(
                            "-ERR Can't execute '%s': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING allowed in this context\r\n",
                            commandName.toLowerCase()
                        );
                        outputStream.write(err.getBytes());
                        outputStream.flush();
                        continue;
                    }
                }

                OutputStream responseStream = isReplicaConnection ? new NullOutputStream() : outputStream;

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

                if (transactionManager.isInTransaction()) {
                    transactionManager.queueCommand(parts, responseStream);
                    continue;
                }

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
        int arrayCount;
        try {
            arrayCount = Integer.parseInt(firstLine.substring(1));
        } catch (NumberFormatException e) {
            return null;
        }

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
