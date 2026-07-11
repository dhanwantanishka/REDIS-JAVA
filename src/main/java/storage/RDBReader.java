package storage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * RDB (Redis Database) file reader
 * Reads and parses RDB files to load persisted data
 */
public class RDBReader {
    
    private static final String RDB_MAGIC = "REDIS";
    
    /**
     * Load data from an RDB file into the Redis store
     * @param directory The directory containing the RDB file
     * @param filename The name of the RDB file
     * @param store The RedisStore to load data into
     * @return true if file was successfully loaded, false if file doesn't exist
     */
    public static boolean loadFromFile(String directory, String filename, RedisStore store) {
        Path filePath = Paths.get(directory, filename);
        
        if (!Files.exists(filePath)) {
            System.out.println("RDB file not found: " + filePath);
            return false;
        }
        
        System.out.println("Loading RDB file: " + filePath);
        
        try (DataInputStream in = new DataInputStream(new FileInputStream(filePath.toFile()))) {
            // Read and verify magic string "REDIS"
            byte[] magic = new byte[5];
            in.readFully(magic);
            if (!RDB_MAGIC.equals(new String(magic))) {
                throw new IOException("Invalid RDB file: bad magic string");
            }
            
            // Read RDB version (4 bytes, ASCII digits)
            byte[] version = new byte[4];
            in.readFully(version);
            System.out.println("RDB version: " + new String(version));
            
            // Parse the RDB file
            parseRDB(in, store);
            
            System.out.println("RDB file loaded successfully");
            return true;
            
        } catch (IOException e) {
            System.err.println("Error loading RDB file: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static void parseRDB(DataInputStream in, RedisStore store) throws IOException {
        while (true) {
            int opcode = in.readUnsignedByte();
            
            switch (opcode) {
                case 0xFF: // EOF
                    System.out.println("Reached end of RDB file");
                    return;
                    
                case 0xFE: // SELECTDB
                    int dbNumber = readLength(in);
                    System.out.println("Selecting database: " + dbNumber);
                    // We only support DB 0 for now
                    break;
                    
                case 0xFD: // EXPIRETIME (seconds)
                    long expireTimeSec = readUnsignedInt(in);
                    long expireTimeMs = expireTimeSec * 1000;
                    readKeyValuePair(in, store, expireTimeMs);
                    break;
                    
                case 0xFC: // EXPIRETIMEMS (milliseconds)
                    long expireTimeMillis = readLong(in);
                    readKeyValuePair(in, store, expireTimeMillis);
                    break;
                    
                case 0xFB: // RESIZEDB
                    int dbHashTableSize = readLength(in);
                    int expiryHashTableSize = readLength(in);
                    System.out.println("DB hash table size: " + dbHashTableSize + ", expiry table size: " + expiryHashTableSize);
                    break;
                    
                case 0xFA: // AUX field
                    String auxKey = readString(in);
                    String auxValue = readString(in);
                    System.out.println("AUX: " + auxKey + " = " + auxValue);
                    break;
                    
                default:
                    // It's a value type, read key-value pair
                    readKeyValuePair(in, store, -1, opcode);
                    break;
            }
        }
    }
    
    private static void readKeyValuePair(DataInputStream in, RedisStore store, long expireTimeMs) throws IOException {
        int valueType = in.readUnsignedByte();
        readKeyValuePair(in, store, expireTimeMs, valueType);
    }
    
    private static void readKeyValuePair(DataInputStream in, RedisStore store, long expireTimeMs, int valueType) throws IOException {
        String key = readString(in);
        
        switch (valueType) {
            case 0: // String encoding
                String value = readString(in);
                if (expireTimeMs > 0) {
                    store.set(key, value, expireTimeMs);
                } else {
                    store.set(key, value, null);
                }
                System.out.println("Loaded key: " + key + " = " + value + 
                                 (expireTimeMs > 0 ? " (expires at " + expireTimeMs + ")" : ""));
                break;
                
            default:
                System.out.println("Unsupported value type: " + valueType + " for key: " + key);
                // Skip unsupported types
                break;
        }
    }
    
    private static int readLength(DataInputStream in) throws IOException {
        int firstByte = in.readUnsignedByte();
        int type = (firstByte & 0xC0) >> 6;
        
        switch (type) {
            case 0: // 6-bit length
                return firstByte & 0x3F;
                
            case 1: // 14-bit length
                int nextByte = in.readUnsignedByte();
                return ((firstByte & 0x3F) << 8) | nextByte;
                
            case 2: // 32-bit length (big-endian)
                // Read 4 bytes as big-endian integer
                int b0 = in.readUnsignedByte();
                int b1 = in.readUnsignedByte();
                int b2 = in.readUnsignedByte();
                int b3 = in.readUnsignedByte();
                return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
                
            case 3: // Special encoding
                int encoding = firstByte & 0x3F;
                switch (encoding) {
                    case 0: // 8-bit integer
                        return in.readUnsignedByte();
                    case 1: // 16-bit integer
                        return readShortLE(in) & 0xFFFF;
                    case 2: // 32-bit integer
                        return readIntLE(in);
                    default:
                        throw new IOException("Unknown special encoding: " + encoding);
                }
                
            default:
                throw new IOException("Unknown length type: " + type);
        }
    }
    
    private static String readString(DataInputStream in) throws IOException {
        int firstByte = in.readUnsignedByte();
        int type = (firstByte & 0xC0) >> 6;
        
        if (type == 3) {
            // Special encoding - integer as string
            int encoding = firstByte & 0x3F;
            switch (encoding) {
                case 0: // 8-bit integer
                    return String.valueOf(in.readByte());
                case 1: // 16-bit integer
                    return String.valueOf(readShortLE(in));
                case 2: // 32-bit integer
                    return String.valueOf(readIntLE(in));
                default:
                    throw new IOException("Unknown string encoding: " + encoding);
            }
        } else {
            // Normal string - read length then bytes
            int length;
            switch (type) {
                case 0: // 6-bit length
                    length = firstByte & 0x3F;
                    break;
                case 1: // 14-bit length
                    int nextByte = in.readUnsignedByte();
                    length = ((firstByte & 0x3F) << 8) | nextByte;
                    break;
                case 2: // 32-bit length (big-endian)
                    int b0 = in.readUnsignedByte();
                    int b1 = in.readUnsignedByte();
                    int b2 = in.readUnsignedByte();
                    int b3 = in.readUnsignedByte();
                    length = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
                    break;
                default:
                    throw new IOException("Unknown length type: " + type);
            }
            
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return new String(bytes);
        }
    }
    
    private static long readUnsignedInt(DataInputStream in) throws IOException {
        // Read 4 bytes in little-endian format
        int b1 = in.readUnsignedByte();
        int b2 = in.readUnsignedByte();
        int b3 = in.readUnsignedByte();
        int b4 = in.readUnsignedByte();
        return (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
    }
    
    private static long readLong(DataInputStream in) throws IOException {
        // Read 8 bytes in little-endian format
        long b1 = in.readUnsignedByte();
        long b2 = in.readUnsignedByte();
        long b3 = in.readUnsignedByte();
        long b4 = in.readUnsignedByte();
        long b5 = in.readUnsignedByte();
        long b6 = in.readUnsignedByte();
        long b7 = in.readUnsignedByte();
        long b8 = in.readUnsignedByte();
        return (b8 << 56) | (b7 << 48) | (b6 << 40) | (b5 << 32) | 
               (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
    }
    
    private static int readIntLE(DataInputStream in) throws IOException {
        // Read 4 bytes in little-endian format
        int b1 = in.readUnsignedByte();
        int b2 = in.readUnsignedByte();
        int b3 = in.readUnsignedByte();
        int b4 = in.readUnsignedByte();
        return (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
    }
    
    private static short readShortLE(DataInputStream in) throws IOException {
        // Read 2 bytes in little-endian format
        int b1 = in.readUnsignedByte();
        int b2 = in.readUnsignedByte();
        return (short)((b2 << 8) | b1);
    }
}