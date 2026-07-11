package command;

import storage.RedisStore;
import protocol.RESPWriter;

import java.io.OutputStream;

/**
 * EXISTS key [key ...]
 * Returns the number of specified keys that exist.
 */
public class ExistsCommand implements Command {
    private final RedisStore store;

    public ExistsCommand(RedisStore store) {
        this.store = store;
    }

    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'exists' command");
            return;
        }

        int count = 0;
        for (int i = 1; i < parts.length; i++) {
            if (store.exists(parts[i])) {
                count++;
            }
        }

        RESPWriter.writeInteger(outputStream, count);
    }
}
