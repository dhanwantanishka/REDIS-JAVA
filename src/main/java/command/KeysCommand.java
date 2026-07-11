package command;

import java.io.OutputStream;
import java.util.List;

import protocol.RESPWriter;
import storage.RedisStore;

public class KeysCommand implements Command {
    private final RedisStore store;

    public KeysCommand(RedisStore store) {
        this.store = store;
    }

    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "ERR wrong number of arguments for 'keys' command");
            return;
        }

        String pattern = parts[1];
        
        // For now, only support "*" pattern
        if (!"*".equals(pattern)) {
            RESPWriter.writeError(outputStream, "ERR only '*' pattern is supported");
            return;
        }

        // Get all keys
        List<String> keys = store.getAllKeys();
        
        // Write as RESP array
        RESPWriter.writeArrayHeader(outputStream, keys.size());
        for (String key : keys) {
            RESPWriter.writeBulkString(outputStream, key);
        }
    }
    
}
