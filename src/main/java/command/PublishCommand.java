package command;

import handler.PubSubManager;
import protocol.RESPWriter;

import java.io.OutputStream;

public class PublishCommand implements Command {
    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        // PUBLISH channel message
        if (parts.length < 3) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'publish' command");
            return;
        }
        String channel = parts[1];
        String message = parts[2];
        int delivered = PubSubManager.getInstance().publish(channel, message);
        RESPWriter.writeInteger(outputStream, delivered);
    }
}


