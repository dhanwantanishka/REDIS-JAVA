package command;

import handler.ReplicationManager;

import java.io.OutputStream;

public class InfoCommand implements Command {
    private final String masterReplId;
    private final Integer masterReplOffset;

    public InfoCommand(String serverRole, String masterReplId, Integer masterReplOffset) {
        // Retain constructor signature for registry compatibility, but ignore serverRole here
        this.masterReplId = masterReplId;
        this.masterReplOffset = masterReplOffset;
    }

    @Override
    public void execute(String[] parts, OutputStream outputStream) throws Exception {
        if (parts.length < 2) {
            outputStream.write("-ERR wrong number of arguments for 'INFO' command\r\n".getBytes());
            outputStream.flush();
            return;
        }

        String section = parts[1].toLowerCase();

        if (section.equals("replication")) {
            String currentRole = ReplicationManager.getInstance().getServerRole();
            StringBuilder info = new StringBuilder();
            info.append("# Replication\r\n");
            info.append("role:").append(currentRole).append("\r\n");
            
            // Only include master_replid and master_repl_offset if this is a master
            if ("master".equals(currentRole) && masterReplId != null) {
                info.append("master_replid:").append(masterReplId).append("\r\n");
                info.append("master_repl_offset:").append(masterReplOffset).append("\r\n");
            }
            
            String infoStr = info.toString();
            outputStream.write(("$" + infoStr.length() + "\r\n" + infoStr + "\r\n").getBytes());
            outputStream.flush();
        } else {
            // For unsupported sections, return empty bulk string
            String info = "";
            outputStream.write(("$" + info.length() + "\r\n" + info + "\r\n").getBytes());
            outputStream.flush();
        }
    }
}
