package command;

import handler.ClientHandler;
import java.io.OutputStream;
import java.net.Socket;

public interface Command {
    void execute(String[] parts, OutputStream outputStream) throws Exception;
    
    // Optional method for commands that need access to the client socket (e.g., PSYNC)
    default void execute(String[] parts, OutputStream outputStream, Socket clientSocket) throws Exception {
        execute(parts, outputStream);
    }
    
    // Optional method for commands that need access to the ClientHandler (e.g., PSYNC)
    default void execute(String[] parts, OutputStream outputStream, Socket clientSocket, ClientHandler clientHandler) throws Exception {
        execute(parts, outputStream, clientSocket);
    }
}