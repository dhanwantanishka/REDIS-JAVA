package command;

import storage.RedisStore;
import protocol.RESPWriter;

import java.io.OutputStream;

/**
 * DEL key [key ...]
 * Removes the specified keys. Returns the number of keys that were removed.
 */
public class DelCommand implements Command {
    private final RedisStore store;

    public DelCommand(RedisStore store) {
        this.store = store;
    }

    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'del' command");
            return;
        }

        int deleted = 0;
        for (int i = 1; i < parts.length; i++) {
            if (store.delete(parts[i])) {
                deleted++;
            }
        }

        RESPWriter.writeInteger(outputStream, deleted);
    }
}
