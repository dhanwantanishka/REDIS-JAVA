package handler;

import java.io.OutputStream;

/**
 * An OutputStream that discards all data written to it.
 * Used when replicas execute commands from master - they shouldn't send responses.
 */
public class NullOutputStream extends OutputStream {
    @Override
    public void write(int b) {
        // Discard
    }
    
    @Override
    public void write(byte[] b) {
        // Discard
    }
    
    @Override
    public void write(byte[] b, int off, int len) {
        // Discard
    }
    
    @Override
    public void flush() {
        // Nothing to flush
    }
    
    @Override
    public void close() {
        // Nothing to close
    }
}