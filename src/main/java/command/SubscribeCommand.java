package command;

import handler.ClientHandler;
import handler.PubSubManager;
import protocol.RESPWriter;
import java.io.OutputStream;
import java.net.Socket;

public class SubscribeCommand implements Command {
    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        // SUBSCRIBE channel [channel ...]
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'subscribe' command");
            return;
        }
        // Simulate per-client subscription for this fallback case using a local Set
        java.util.Set<String> subChannels = new java.util.HashSet<>();
        int count = 0;
        for (int i = 1; i < parts.length; i++) {
            String channel = parts[i];
            // Only increase count for a newly-subscribed channel
            if (subChannels.add(channel)) {
                count++;
            }
            PubSubManager.getInstance().subscribe(channel, outputStream);
            // RESP Array: ["subscribe", channel, count]
            RESPWriter.writeArrayHeader(outputStream, 3);
            RESPWriter.writeBulkString(outputStream, "subscribe");
            RESPWriter.writeBulkString(outputStream, channel);
            RESPWriter.writeInteger(outputStream, subChannels.size()); // Total count after subscription
        }
    }

    @Override
    public void execute(String[] parts, OutputStream outputStream, Socket clientSocket, ClientHandler clientHandler) throws Exception {
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'subscribe' command");
            return;
        }
        for (int i = 1; i < parts.length; i++) {
            String channel = parts[i];
            // Only increment subscription count if newly subscribed
            int count = clientHandler.subscribeChannel(channel); // Returns updated count after subscription
            PubSubManager.getInstance().subscribe(channel, outputStream);
            // RESP Array: ["subscribe", channel, count]
            RESPWriter.writeArrayHeader(outputStream, 3);
            RESPWriter.writeBulkString(outputStream, "subscribe");
            RESPWriter.writeBulkString(outputStream, channel);
            RESPWriter.writeInteger(outputStream, count); // Total count after subscription
        }
    }
}
