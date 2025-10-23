package handler;

import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

public class ReplicaConnection {
    private final Socket socket;
    private final OutputStream outputStream;
    private final String replicaId;
    private int offset;
    private CompletableFuture<Integer> pendingAck;
    
    public ReplicaConnection(Socket socket, OutputStream outputStream) {
        this.socket = socket;
        this.outputStream = outputStream;
        this.replicaId = socket.getRemoteSocketAddress().toString();
        this.offset = 0;
        this.pendingAck = null;
    }
    
    public Socket getSocket() {
        return socket;
    }
    
    public OutputStream getOutputStream() {
        return outputStream;
    }
    
    public String getReplicaId() {
        return replicaId;
    }
    
    public int getOffset() {
        return offset;
    }
    
    public void incrementOffset(int bytes) {
        this.offset += bytes;
    }
    
    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }
    
    /**
     * Set up a future to receive the next ACK response
     */
    public synchronized CompletableFuture<Integer> createAckFuture() {
        pendingAck = new CompletableFuture<>();
        return pendingAck;
    }
    
    /**
     * Complete the pending ACK future with the reported offset
     */
    public synchronized void completeAck(int reportedOffset) {
        if (pendingAck != null && !pendingAck.isDone()) {
            pendingAck.complete(reportedOffset);
        }
    }
    
    /**
     * Check if there's a pending ACK request
     */
    public synchronized boolean hasPendingAck() {
        return pendingAck != null && !pendingAck.isDone();
    }
    
    @Override
    public String toString() {
        return "Replica[" + replicaId + ", offset=" + offset + "]";
    }
}