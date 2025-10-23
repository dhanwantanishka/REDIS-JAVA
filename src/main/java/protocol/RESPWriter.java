package protocol;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Utility class for writing RESP (REdis Serialization Protocol) formatted data
 */
public class RESPWriter {
    
    private RESPWriter() {
        // Utility class, prevent instantiation
    }
    
    /**
     * Write a simple string response: +OK\r\n
     */
    public static void writeSimpleString(OutputStream out, String message) throws IOException {
        out.write(("+"+message+"\r\n").getBytes());
        out.flush();
    }
    
    /**
     * Write an error response: -ERR message\r\n
     */
    public static void writeError(OutputStream out, String message) throws IOException {
        out.write(("-ERR "+message+"\r\n").getBytes());
        out.flush();
    }
    
    /**
     * Write an integer response: :123\r\n
     */
    public static void writeInteger(OutputStream out, long value) throws IOException {
        out.write((":"+value+"\r\n").getBytes());
        out.flush();
    }
    
    /**
     * Write a bulk string response: $6\r\nfoobar\r\n
     */
    public static void writeBulkString(OutputStream out, String value) throws IOException {
        if (value == null) {
            out.write("$-1\r\n".getBytes());
        } else {
            out.write(("$"+value.length()+"\r\n"+value+"\r\n").getBytes());
        }
        out.flush();
    }
    
    /**
     * Write a null bulk string: $-1\r\n
     */
    public static void writeNullBulkString(OutputStream out) throws IOException {
        out.write("$-1\r\n".getBytes());
        out.flush();
    }
    
    /**
     * Write an array header: *3\r\n
     */
    public static void writeArrayHeader(OutputStream out, int count) throws IOException {
        out.write(("*"+count+"\r\n").getBytes());
    }
    
    /**
     * Build a RESP command array as a string
     * Example: buildCommand("SET", "key", "value") -> "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
     */
    public static String buildCommand(String... parts) {
        StringBuilder command = new StringBuilder();
        command.append("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            command.append("$").append(part.length()).append("\r\n");
            command.append(part).append("\r\n");
        }
        return command.toString();
    }
    
    /**
     * Write a RESP command array directly to the output stream
     */
    public static void writeCommand(OutputStream out, String... parts) throws IOException {
        out.write(buildCommand(parts).getBytes());
        out.flush();
    }
}