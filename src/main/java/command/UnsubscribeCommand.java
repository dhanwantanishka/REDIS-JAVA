package command;

import handler.ClientHandler;
import handler.PubSubManager;
import protocol.RESPWriter;

import java.io.OutputStream;
import java.net.Socket;

public class UnsubscribeCommand implements Command {
    @Override
    public void execute(String[] parts, OutputStream outputStream, Socket clientSocket, ClientHandler clientHandler) throws Exception {
        int count;
        // If no channels specified, unsubscribe from all
        if (parts.length == 1) {
            for (String channel : clientHandler.getChannels()) {
                clientHandler.unsubscribeChannel(channel);
                PubSubManager.getInstance().unsubscribe(channel, outputStream);
                count = clientHandler.getChannels().size();
                RESPWriter.writeArrayHeader(outputStream, 3);
                RESPWriter.writeBulkString(outputStream, "unsubscribe");
                RESPWriter.writeBulkString(outputStream, channel);
                RESPWriter.writeInteger(outputStream, count);
            }
        } else {
            // Unsubscribe from specified channels
            for (int i = 1; i < parts.length; i++) {
                String channel = parts[i];
                clientHandler.unsubscribeChannel(channel);
                PubSubManager.getInstance().unsubscribe(channel, outputStream);
                count = clientHandler.getChannels().size();
                RESPWriter.writeArrayHeader(outputStream, 3);
                RESPWriter.writeBulkString(outputStream, "unsubscribe");
                RESPWriter.writeBulkString(outputStream, channel);
                RESPWriter.writeInteger(outputStream, count);
            }
        }
    }

    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execute'");
    }
}
