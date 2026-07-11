package command;

import handler.ReplicaConnection;
import handler.ReplicationManager;
import protocol.RESPConstants;
import protocol.RESPWriter;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WaitCommand implements Command {
    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        if (parts.length < 3) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'wait' command");
            return;
        }
        
        int numReplicas;
        long timeout;
        
        try {
            numReplicas = Integer.parseInt(parts[1]);
            timeout = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            RESPWriter.writeError(outputStream, "value is not an integer or out of range");
            return;
        }
        
        ReplicationManager replicationManager = ReplicationManager.getInstance();
        List<ReplicaConnection> replicas = replicationManager.getReplicas();
        
        // If there are no replicas, return 0 immediately
        if (replicas.isEmpty()) {
            RESPWriter.writeInteger(outputStream, 0);
            return;
        }
        
        // If numReplicas is 0, return immediately with the number of connected replicas
        if (numReplicas == 0) {
            RESPWriter.writeInteger(outputStream, replicas.size());
            return;
        }
        
        // Quick check: if no writes have been propagated (all offsets are 0), return immediately
        boolean anyWrites = false;
        for (ReplicaConnection replica : replicas) {
            if (replica.getOffset() > 0) {
                anyWrites = true;
                break;
            }
        }
        
        if (!anyWrites) {
            // No writes have been propagated, all replicas are caught up by definition
            // Return the actual number of replicas (they're all caught up)
            RESPWriter.writeInteger(outputStream, replicas.size());
            return;
        }
        
        // Send REPLCONF GETACK * to all replicas and set up futures
        List<CompletableFuture<Integer>> ackFutures = new ArrayList<>();
        
        for (ReplicaConnection replica : replicas) {
            try {
                if (replica.isConnected()) {
                    // Create a future for this replica's ACK
                    CompletableFuture<Integer> ackFuture = replica.createAckFuture();
                    ackFutures.add(ackFuture);
                    
                    // Send REPLCONF GETACK *
                    replica.getOutputStream().write(RESPConstants.REPLCONF_GETACK.getBytes());
                    replica.getOutputStream().flush();
                }
            } catch (Exception e) {
                System.err.println("WAIT: Failed to send GETACK: " + e.getMessage());
            }
        }
        
        // Wait for ACK responses (with shared timeout across all replicas)
        int ackedCount = 0;
        List<ReplicaConnection> replicaList = replicationManager.getReplicas();
        long deadline = System.currentTimeMillis() + timeout;
        
        for (int i = 0; i < ackFutures.size() && i < replicaList.size(); i++) {
            CompletableFuture<Integer> ackFuture = ackFutures.get(i);
            ReplicaConnection replica = replicaList.get(i);
            
            try {
                // Calculate remaining time
                long remainingTime = deadline - System.currentTimeMillis();
                if (remainingTime <= 0) {
                    break; // Overall timeout reached
                }
                
                // Wait for the ACK with remaining timeout
                Integer reportedOffset = ackFuture.get(remainingTime, TimeUnit.MILLISECONDS);
                int expectedOffset = replica.getOffset();
                
                // Check if the replica is caught up
                if (reportedOffset >= expectedOffset) {
                    ackedCount++;
                    if (ackedCount >= numReplicas) {
                        break; // We have enough acknowledgments
                    }
                }
            } catch (java.util.concurrent.TimeoutException e) {
                // Timeout waiting for this replica, continue to next
            } catch (Exception e) {
                System.err.println("WAIT: Error from " + replica.getReplicaId() + ": " + e.getMessage());
            }
        }
        
        RESPWriter.writeInteger(outputStream, ackedCount);
    }
}
