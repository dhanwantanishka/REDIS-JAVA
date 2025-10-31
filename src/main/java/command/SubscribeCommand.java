package command;

import protocol.RESPWriter;

import java.io.OutputStream;

public class SubscribeCommand implements Command {
    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        // SUBSCRIBE channel [channel ...]
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'subscribe' command");
            return;
        }

        int subscriptionCount = 0;
        for (int i = 1; i < parts.length; i++) {
            String channel = parts[i];
            subscriptionCount++;

            // Reply: ["subscribe", channel, subscriptionCount]
            RESPWriter.writeArrayHeader(outputStream, 3);
            RESPWriter.writeBulkString(outputStream, "subscribe");
            RESPWriter.writeBulkString(outputStream, channel);
            RESPWriter.writeInteger(outputStream, subscriptionCount);
        }
    }
}


