package handler;

import protocol.RESPWriter;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class PubSubManager {
    private static final PubSubManager INSTANCE = new PubSubManager();

    private final Map<String, CopyOnWriteArrayList<OutputStream>> channelToSubscribers = new ConcurrentHashMap<>();

    private PubSubManager() {}

    public static PubSubManager getInstance() {
        return INSTANCE;
    }

    public void subscribe(String channel, OutputStream out) {
        channelToSubscribers
            .computeIfAbsent(channel, key -> new CopyOnWriteArrayList<>())
            .addIfAbsent(out);
    }

    public void unsubscribe(String channel, OutputStream out) {
        List<OutputStream> subscribers = channelToSubscribers.get(channel);
        if (subscribers != null) {
            subscribers.remove(out);
            if (subscribers.isEmpty()) {
                channelToSubscribers.remove(channel, subscribers);
            }
        }
    }

    public int publish(String channel, String message) {
        List<OutputStream> subscribers = channelToSubscribers.get(channel);
        if (subscribers == null || subscribers.isEmpty()) {
            return 0;
        }

        int delivered = 0;
        for (OutputStream out : subscribers) {
            try {
                // Send RESP Array: ["message", channel, message]
                RESPWriter.writeArrayHeader(out, 3);
                RESPWriter.writeBulkString(out, "message");
                RESPWriter.writeBulkString(out, channel);
                RESPWriter.writeBulkString(out, message);
                delivered++;
            } catch (Exception ignored) {
                // Ignore exceptions to keep publishing resilient
            }
        }
        return delivered;
    }
}
