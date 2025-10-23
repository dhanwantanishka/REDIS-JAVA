package command;

import protocol.RESPWriter;

import java.io.OutputStream;

public class ConfigCommand implements Command {
    private final String rdbDir;
    private final String rdbFilename;
    
    public ConfigCommand(String rdbDir, String rdbFilename) {
        this.rdbDir = rdbDir;
        this.rdbFilename = rdbFilename;
    }
    
    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        if (parts.length < 2) {
            RESPWriter.writeError(outputStream, "wrong number of arguments for 'config' command");
            return;
        }
        
        String subcommand = parts[1].toLowerCase();
        
        switch (subcommand) {
            case "get":
                if (parts.length < 3) {
                    RESPWriter.writeError(outputStream, "wrong number of arguments for 'config get'");
                    return;
                }
                handleConfigGet(parts[2], outputStream);
                break;
                
            default:
                RESPWriter.writeError(outputStream, "unknown CONFIG subcommand '" + parts[1] + "'");
                break;
        }
    }
    
    private void handleConfigGet(String parameter, OutputStream outputStream) throws Exception {
        parameter = parameter.toLowerCase();
        
        switch (parameter) {
            case "dir":
                // Return array: [parameter_name, parameter_value]
                RESPWriter.writeArrayHeader(outputStream, 2);
                RESPWriter.writeBulkString(outputStream, "dir");
                RESPWriter.writeBulkString(outputStream, rdbDir);
                break;
                
            case "dbfilename":
                // Return array: [parameter_name, parameter_value]
                RESPWriter.writeArrayHeader(outputStream, 2);
                RESPWriter.writeBulkString(outputStream, "dbfilename");
                RESPWriter.writeBulkString(outputStream, rdbFilename);
                break;
                
            default:
                // Unknown parameter, return empty array
                RESPWriter.writeArrayHeader(outputStream, 0);
                outputStream.flush();
                break;
        }
    }
}