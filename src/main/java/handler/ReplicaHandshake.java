package handler;

import command.Command;
import command.CommandRegistry;

import java.io.*;
import java.net.Socket;

public class ReplicaHandshake {
    private final String masterHost;
    private final int masterPort;
    private final int replicaPort;
    private final CommandRegistry commandRegistry;

    public ReplicaHandshake(String masterHost, int masterPort, int replicaPort, CommandRegistry commandRegistry) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.replicaPort = replicaPort;
        this.commandRegistry = commandRegistry;
    }

    public void performHandshake() {
        Socket socket = null;
        try {
            socket = new Socket(masterHost, masterPort);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            System.out.println("Initiating handshake with master at " + masterHost + ":" + masterPort);

            // Step 1: Send PING
            sendCommand(out, "PING");
            String response = readSimpleResponse(in);
            System.out.println("PING response: " + response);
            if (!response.equals("PONG")) {
                throw new IOException("Unexpected PING response: " + response);
            }

            // Step 2: Send REPLCONF listening-port <PORT>
            sendCommand(out, "REPLCONF", "listening-port", String.valueOf(replicaPort));
            response = readSimpleResponse(in);
            System.out.println("REPLCONF listening-port response: " + response);
            if (!response.equals("OK")) {
                throw new IOException("Unexpected REPLCONF listening-port response: " + response);
            }

            // Step 3: Send REPLCONF capa psync2
            sendCommand(out, "REPLCONF", "capa", "psync2");
            response = readSimpleResponse(in);
            System.out.println("REPLCONF capa response: " + response);
            if (!response.equals("OK")) {
                throw new IOException("Unexpected REPLCONF capa response: " + response);
            }

            // Step 4: Send PSYNC ? -1
            sendCommand(out, "PSYNC", "?", "-1");
            response = readSimpleResponse(in);
            System.out.println("PSYNC response: " + response);
            
            // Expect: +FULLRESYNC <REPL_ID> <OFFSET>
            if (response != null && response.startsWith("FULLRESYNC")) {
                System.out.println("Handshake completed successfully!");
                
                // Read the RDB file using raw input stream (binary data)
                readRDBFile(in);
                System.out.println("Received RDB file from master");
                
                System.out.println("Replica is now connected to master");
                System.out.println("Listening for propagated commands...");
                
                // Keep connection open and process commands from master
                // Create BufferedReader for reading text commands
                BufferedReader commandReader = new BufferedReader(new InputStreamReader(in));
                listenForCommands(socket, commandReader, out);
            } else {
                throw new IOException("Unexpected PSYNC response: " + response);
            }

        } catch (IOException e) {
            System.err.println("Handshake failed: " + e.getMessage());
            e.printStackTrace();
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ex) {
                    // Ignore
                }
            }
        }
    }
    
    private String readSimpleResponse(InputStream in) throws IOException {
        // Read a line from input stream without BufferedReader
        // Format: +OK\r\n or -ERR message\r\n
        StringBuilder line = new StringBuilder();
        int b;
        
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') {
                    break;
                }
                line.append((char) b);
                line.append((char) next);
            } else {
                line.append((char) b);
            }
        }
        
        String response = line.toString();
        if (response.startsWith("+")) {
            return response.substring(1);
        } else if (response.startsWith("-")) {
            throw new IOException("Error from master: " + response.substring(1));
        }
        throw new IOException("Unexpected response format: " + response);
    }
    
    private void readRDBFile(InputStream in) throws IOException {
        // Read the bulk string header: $<length>\r\n
        // We need to read byte by byte until we get the length
        StringBuilder lengthStr = new StringBuilder();
        int b = in.read();
        if (b != '$') {
            throw new IOException("Expected RDB bulk string to start with $, got: " + (char)b);
        }
        
        // Read until \r\n
        while (true) {
            b = in.read();
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') {
                    break;
                }
                lengthStr.append((char)b);
                lengthStr.append((char)next);
            } else {
                lengthStr.append((char)b);
            }
        }
        
        int length = Integer.parseInt(lengthStr.toString());
        System.out.println("RDB length: " + length);
        
        // Read the RDB data (binary)
        byte[] rdbData = new byte[length];
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(rdbData, totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected EOF while reading RDB file");
            }
            totalRead += read;
        }
        
        System.out.println("Read RDB file: " + length + " bytes");
    }
    
    private void listenForCommands(Socket socket, BufferedReader in, OutputStream out) {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                if (!line.startsWith("*")) {
                    System.out.println("Skipping non-array line: " + line);
                    continue;
                }
                
                // Calculate bytes for this command (for offset tracking)
                int commandBytes = line.length() + 2; // +2 for \r\n
                
                // Parse RESP array
                String[] parts = parseLine(line, in);
                
                if (parts == null || parts.length == 0) {
                    System.out.println("Parsed null or empty command");
                    continue;
                }
                
                // Calculate total bytes read for this command
                for (String part : parts) {
                    if (part != null) {
                        commandBytes += 1 + String.valueOf(part.length()).length() + 2; // $<len>\r\n
                        commandBytes += part.length() + 2; // <data>\r\n
                    }
                }
                
                String commandName = parts[0].toUpperCase();
                
                // Execute command
                Command command = commandRegistry.getCommand(commandName);
                if (command != null) {
                    try {
                        // REPLCONF GETACK needs to respond back to master, other commands don't
                        boolean isGetAck = commandName.equals("REPLCONF") && 
                                          parts.length >= 2 && 
                                          parts[1].equalsIgnoreCase("GETACK");
                        
                        OutputStream responseStream = isGetAck ? out : new NullOutputStream();
                        command.execute(parts, responseStream);
                        
                        // Increment offset AFTER execution for all commands
                        ReplicationManager.getInstance().incrementReplicaOffset(commandBytes);
                    } catch (Exception e) {
                        System.err.println("Error executing command " + commandName + ": " + e.getMessage());
                    }
                } else {
                    System.out.println("Unknown command from master: " + commandName);
                }
            }
            
            System.out.println("Master connection closed");
        } catch (Exception e) {
            System.err.println("Error processing commands from master: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore
            }
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

    private void sendCommand(OutputStream out, String... parts) throws IOException {
        StringBuilder command = new StringBuilder();
        command.append("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            command.append("$").append(part.length()).append("\r\n");
            command.append(part).append("\r\n");
        }
        out.write(command.toString().getBytes());
        out.flush();
        System.out.println("Sent: " + String.join(" ", parts));
    }
}