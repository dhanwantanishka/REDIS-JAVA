package command;

import storage.RedisStore;
import java.util.HashMap;
import java.util.Map;

public class CommandRegistry {
    private final Map<String, Command> commands = new HashMap<>();
    private final String serverRole;

    public CommandRegistry(RedisStore store, String serverRole, String masterReplId, Integer masterReplOffset, 
                          String rdbDir, String rdbFilename) {
        this.serverRole = serverRole;
        // Commands that don't need store
        commands.put("PING", new PingCommand());
        commands.put("ECHO", new EchoCommand());
        
        // Commands that need server info
        commands.put("INFO", new InfoCommand(serverRole, masterReplId, masterReplOffset));
        commands.put("CONFIG", new ConfigCommand(rdbDir, rdbFilename));
        
        // Replication commands
        commands.put("REPLCONF", new ReplconfCommand());
        commands.put("PSYNC", new PsyncCommand(masterReplId, masterReplOffset));
        
        // Commands that need store
        commands.put("SET", new SetCommand(store));
        commands.put("GET", new GetCommand(store));
        commands.put("INCR", new IncrCommand(store));
        commands.put("RPUSH", new RPushCommand(store));
        commands.put("LPUSH", new LPushCommand(store));
        commands.put("LRANGE", new LRangeCommand(store));
        commands.put("LLEN", new LLEnCommand(store));
        commands.put("LPOP", new LPopCommand(store));
        commands.put("BLPOP", new BLPopCommand(store));
        commands.put("TYPE", new TypeCommand(store));
        commands.put("XADD", new XAddCommand(store));
        commands.put("XRANGE", new XRangeCommand(store));
        commands.put("XREAD", new XReadCommand(store));
        commands.put("KEYS", new KeysCommand(store));
        
        // Replication synchronization
        commands.put("WAIT", new WaitCommand());
    }

    public Command getCommand(String commandName) {
        return commands.get(commandName.toUpperCase());
    }
    
    public String getServerRole() {
        return serverRole;
    }
    
    public boolean isMaster() {
        return "master".equals(serverRole);
    }
}
