package command;

import handler.ClientHandler;
import handler.ReplicationManager;
import protocol.RESPConstants;

import java.io.OutputStream;
import java.net.Socket;

public class PsyncCommand implements Command {
    private final String masterReplId;
    private final Integer masterReplOffset;
    
    public PsyncCommand(String masterReplId, Integer masterReplOffset) {
        this.masterReplId = masterReplId;
        this.masterReplOffset = masterReplOffset;
    }
    
    @Override
    public void execute(String[] parts, OutputStream outputStream, Socket clientSocket, ClientHandler clientHandler) throws Exception {
        // PSYNC <replication-id> <offset>
        // PSYNC ? -1 means the replica wants a full resync
        
        if (parts.length < 3) {
            outputStream.write("-ERR wrong number of arguments for 'psync' command\r\n".getBytes());
            outputStream.flush();
            return;
        }
        
        String replicationId = parts[1];
        String offset = parts[2];
        
        System.out.println("PSYNC request: replicationId=" + replicationId + ", offset=" + offset);
        
        // If replica sends "?" or "-1", it wants a full resync
        if (replicationId.equals("?") || offset.equals("-1")) {
            // Send FULLRESYNC response
            String response = String.format("+FULLRESYNC %s %d\r\n", masterReplId, masterReplOffset);
            outputStream.write(response.getBytes());
            outputStream.flush();
            
            System.out.println("Sent FULLRESYNC: " + masterReplId + " " + masterReplOffset);
            
            // Send empty RDB file
            sendEmptyRDB(outputStream);
            
            // Register this replica for command propagation
            ReplicationManager.getInstance().addReplica(clientSocket, outputStream);
            
            // Mark this connection as a replica connection so it doesn't send responses
            clientHandler.markAsReplicaConnection();
            
        } else {
            // Partial resync requested - for now, always do full resync
            String response = String.format("+FULLRESYNC %s %d\r\n", masterReplId, masterReplOffset);
            outputStream.write(response.getBytes());
            outputStream.flush();
            
            System.out.println("Sent FULLRESYNC (partial resync not supported): " + masterReplId + " " + masterReplOffset);
            sendEmptyRDB(outputStream);
            
            // Register this replica for command propagation
            ReplicationManager.getInstance().addReplica(clientSocket, outputStream);
            
            // Mark this connection as a replica connection so it doesn't send responses
            clientHandler.markAsReplicaConnection();
        }
    }
    
    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        // This should not be called without socket, but provide fallback
        throw new UnsupportedOperationException("PSYNC requires socket access");
    }
    
    private void sendEmptyRDB(OutputStream outputStream) throws Exception {
        byte[] emptyRDB = hexStringToByteArray(RESPConstants.EMPTY_RDB_HEX);
        
        // Send as bulk string: $<length>\r\n<data>
        String header = String.format("$%d\r\n", emptyRDB.length);
        outputStream.write(header.getBytes());
        outputStream.write(emptyRDB);
        outputStream.flush();
        
        System.out.println("Sent empty RDB file (" + emptyRDB.length + " bytes)");
    }
    
    private byte[] hexStringToByteArray(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}