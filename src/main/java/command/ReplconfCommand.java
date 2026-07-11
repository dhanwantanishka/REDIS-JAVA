package command;

import handler.ReplicationManager;
import protocol.RESPConstants;
import protocol.RESPWriter;

import java.io.OutputStream;

public class ReplconfCommand implements Command {
    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        // REPLCONF has various subcommands:
        // REPLCONF listening-port <PORT>
        // REPLCONF capa <capability>
        // REPLCONF getack <offset>
        
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'replconf' command");
            return;
        }
        
        String subcommand = parts[1].toLowerCase();
        
        switch (subcommand) {
            case "listening-port":
                if (parts.length < 3) {
                    RESPWriter.writeError(outputStream, "wrong number of arguments for 'replconf listening-port'");
                } else {
                    outputStream.write(RESPConstants.OK.getBytes());
                }
                break;
                
            case "capa":
                if (parts.length < 3) {
                    RESPWriter.writeError(outputStream, "wrong number of arguments for 'replconf capa'");
                } else {
                    outputStream.write(RESPConstants.OK.getBytes());
                }
                break;
                
            case "getack":
                // Master is asking for our replication offset
                // Respond with: REPLCONF ACK <offset>
                int offset = ReplicationManager.getInstance().getReplicaOffset();
                String offsetStr = String.valueOf(offset);
                
                // Build RESP array: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
                RESPWriter.writeArrayHeader(outputStream, 3);
                RESPWriter.writeBulkString(outputStream, "REPLCONF");
                RESPWriter.writeBulkString(outputStream, "ACK");
                RESPWriter.writeBulkString(outputStream, offsetStr);
                break;
                
            default:
                RESPWriter.writeError(outputStream, "unknown REPLCONF subcommand '" + parts[1] + "'");
                break;
        }
        
        outputStream.flush();
    }
}