package protocol;

/**
 * Constants for RESP (REdis Serialization Protocol)
 */
public class RESPConstants {
    
    // RESP type markers
    public static final char SIMPLE_STRING = '+';
    public static final char ERROR = '-';
    public static final char INTEGER = ':';
    public static final char BULK_STRING = '$';
    public static final char ARRAY = '*';
    
    // Common responses
    public static final String OK = "+OK\r\n";
    public static final String PONG = "+PONG\r\n";
    public static final String NULL_BULK_STRING = "$-1\r\n";
    public static final String EMPTY_ARRAY = "*0\r\n";
    
    // Line terminators
    public static final String CRLF = "\r\n";
    public static final byte[] CRLF_BYTES = CRLF.getBytes();
    
    // Replication constants
    public static final String REPLCONF_GETACK = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    public static final int REPL_ID_LENGTH = 40;
    
    // RDB constants
    public static final String EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    
    private RESPConstants() {
        // Utility class, prevent instantiation
    }
}