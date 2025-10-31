package handler;

import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReplicationManager {
    private static final ReplicationManager INSTANCE = new ReplicationManager();
    private final List<ReplicaConnection> replicas = new CopyOnWriteArrayList<>();
    private String serverRole = "master"; // Default to master
    private int replicaOffset = 0; // Offset of this server when it's a replica
    
    private ReplicationManager() {
        // Private constructor for singleton
    }
    
    public static ReplicationManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * Set the server role (master or slave)
     */
    public void setServerRole(String role) {
        this.serverRole = role;
        System.out.println("ReplicationManager: Server role set to " + role);
    }
    
    public String getServerRole() {
        return serverRole;
    }
    
    /**
     * Register a new replica connection after successful handshake
     */
    public void addReplica(Socket socket, OutputStream outputStream) {
        ReplicaConnection replica = new ReplicaConnection(socket, outputStream);
        replicas.add(replica);
        System.out.println("Replica registered: " + replica + " (total: " + replicas.size() + ")");
    }
    
    /**
     * Remove a replica connection (e.g., when disconnected)
     */
    public void removeReplica(ReplicaConnection replica) {
        replicas.remove(replica);
        System.out.println("Replica removed: " + replica + " (total: " + replicas.size() + ")");
    }
    
    /**
     * Propagate a write command to all connected replicas
     * @param commandParts The command parts (e.g., ["SET", "key", "value"])
     */
    public void propagateCommand(String[] commandParts) {
        // Only propagate if this is a master server
        if (!"master".equals(serverRole)) {
            return;
        }
        
        if (replicas.isEmpty()) {
            return;
        }
        
        // Build RESP array command
        StringBuilder command = new StringBuilder();
        command.append("*").append(commandParts.length).append("\r\n");
        for (String part : commandParts) {
            command.append("$").append(part.length()).append("\r\n");
            command.append(part).append("\r\n");
        }
        
        byte[] commandBytes = command.toString().getBytes();
        
        // Send to all replicas
        List<ReplicaConnection> disconnected = new ArrayList<>();
        for (ReplicaConnection replica : replicas) {
            try {
                if (replica.isConnected()) {
                    replica.getOutputStream().write(commandBytes);
                    replica.getOutputStream().flush();
                    replica.incrementOffset(commandBytes.length);
                } else {
                    disconnected.add(replica);
                }
            } catch (Exception e) {
                System.err.println("Failed to propagate to " + replica.getReplicaId() + ": " + e.getMessage());
                disconnected.add(replica);
            }
        }
        
        // Clean up disconnected replicas
        for (ReplicaConnection replica : disconnected) {
            removeReplica(replica);
        }
    }
    
    /**
     * Get the number of connected replicas
     */
    public int getReplicaCount() {
        return replicas.size();
    }
    
    /**
     * Get all connected replicas
     */
    public List<ReplicaConnection> getReplicas() {
        return new ArrayList<>(replicas);
    }
    
    /**
     * Get the replication offset (for replica servers)
     */
    public int getReplicaOffset() {
        return replicaOffset;
    }
    
    /**
     * Increment the replication offset by the given number of bytes
     */
    public void incrementReplicaOffset(int bytes) {
        this.replicaOffset += bytes;
    }
}