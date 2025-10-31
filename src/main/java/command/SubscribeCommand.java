package command;

import handler.ClientHandler;
import protocol.RESPWriter;

import java.io.OutputStream;
import java.net.Socket;

public class SubscribeCommand implements Command {
    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        // Fallback if handler-overload isn't used; without per-connection state we treat all as first
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'subscribe' command");
            return;
        }
        int count = 0;
        for (int i = 1; i < parts.length; i++) {
            count++;
            RESPWriter.writeArrayHeader(outputStream, 3);
            RESPWriter.writeBulkString(outputStream, "subscribe");
            RESPWriter.writeBulkString(outputStream, parts[i]);
            RESPWriter.writeInteger(outputStream, count);
        }
    }
    @Override
    public void execute(String[] parts, OutputStream outputStream, Socket clientSocket, ClientHandler clientHandler) throws Exception {
        // SUBSCRIBE channel [channel ...]
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'subscribe' command");
            return;
        }

        for (int i = 1; i < parts.length; i++) {
            String channel = parts[i];
            int count = clientHandler.subscribeChannel(channel);

            // Reply: ["subscribe", channel, count]
            RESPWriter.writeArrayHeader(outputStream, 3);
            RESPWriter.writeBulkString(outputStream, "subscribe");
            RESPWriter.writeBulkString(outputStream, channel);
            RESPWriter.writeInteger(outputStream, count);
        }
    }
}


