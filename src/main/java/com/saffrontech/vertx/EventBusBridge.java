package com.saffrontech.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple event bus bridge using Vert.x websockets.
 * Note: Only Message&lt;JsonObject&gt; and Message<lt;String&gt; is supported
 *
 * Created by beders on 6/23/15.
 */
public class EventBusBridge {
    Vertx vertx;
    WebSocket webSocket;
    long pingTimerID;
    ConcurrentHashMap<String, List<DefaultHandler<?>>> handlers = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, DefaultHandler<?>> replyHandlers = new ConcurrentHashMap<>();

    static final int MAX_SOCKET_FRAME_SIZE = 2*(int)Math.pow(2,18); // 512K max payload

    public static EventBusBridge connect(URI endPoint, io.vertx.core.Handler<EventBusBridge> onOpenHandler) {
        return connect(endPoint, onOpenHandler, MAX_SOCKET_FRAME_SIZE);
    }

    public static EventBusBridge connect(URI endPoint, io.vertx.core.Handler<EventBusBridge> onOpenHandler, int maxSocketFrameSize) {
        return new EventBusBridge(endPoint, onOpenHandler, maxSocketFrameSize);
    }

    private EventBusBridge(URI endPoint, io.vertx.core.Handler<EventBusBridge> onOpenHandler, int maxSocketFrameSize) {
        vertx = Vertx.vertx();
        vertx.createHttpClient(new HttpClientOptions().setMaxWebsocketFrameSize(maxSocketFrameSize)).websocket(endPoint.getPort(), endPoint.getHost(), endPoint.getPath() + "/websocket", ws -> {
            webSocket = ws;
            ws.handler(this::bufferReceived);
            ws.closeHandler(it -> {
                if (pingTimerID != 0) {
                    vertx.cancelTimer(pingTimerID);
                }
                handlers.clear();
                replyHandlers.clear();
            });
            onOpenHandler.handle(EventBusBridge.this);
            sendPing();
            pingTimerID = vertx.setPeriodic(5000L, time -> sendPing());

        });
    }

    protected void sendPing() {
        //System.out.println("Sending ping");
        if (webSocket != null) {
            JsonObject msg = new JsonObject().put("type", "ping");
            try {
                webSocket.write(Buffer.buffer(msg.toString()));
            } catch (IllegalStateException ise) {
                vertx.cancelTimer(pingTimerID);
            }
        } else if (pingTimerID != 0) {
            vertx.cancelTimer(pingTimerID);
        }
    }

    public EventBusBridge send(String address, String message) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, null);
        return this;
    }

    public EventBusBridge publish(String address, String message) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, null);
        return this;
    }

    public EventBusBridge send(String address, String message, EventHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, replyHandler);
        return this;
    }

    public EventBusBridge publish(String address, String message, EventHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, replyHandler);
        return this;
    }

    public EventBusBridge send(String address, String message, MessageHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, replyHandler);
        return this;
    }

    public EventBusBridge publish(String address, String message, MessageHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, replyHandler);
        return this;
    }

    public EventBusBridge send(String address, JsonObject message) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, null);
        return this;
    }

    public EventBusBridge publish(String address, JsonObject message) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, null);
        return this;
    }

    public EventBusBridge send(String address, JsonObject message, EventHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, replyHandler);
        return this;
    }

    public EventBusBridge publish(String address, JsonObject message, EventHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, replyHandler);
        return this;
    }

    public EventBusBridge send(String address, JsonObject message, MessageHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, replyHandler);
        return this;
    }

    public EventBusBridge publish(String address, JsonObject message, MessageHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, replyHandler);
        return this;
    }

    public EventBusBridge registerHandler(String address, MessageHandler<?> messageHandler) {
        return registerHandlerInternal(address, messageHandler);
    }

    public EventBusBridge registerHandler(String address, EventHandler<?> eventHandler) {
        return registerHandlerInternal(address, eventHandler);
    }

    protected EventBusBridge registerHandlerInternal(String address, DefaultHandler<?> eventHandler) {
        handlers.computeIfAbsent(address, key -> {
            webSocket.write(Buffer.buffer(new JsonObject().put("type", "register").put("address", address).toString()));
            return Collections.synchronizedList(new ArrayList<>());
        }).add(eventHandler);
        return this;
    }

    public EventBusBridge unregisterHandler(String address, MessageHandler<?> messageHandler) {
        return unregisterHandlerInternal(address, messageHandler);
    }

    public EventBusBridge unregisterHandler(String address, EventHandler<?> eventHandler) {
        return unregisterHandlerInternal(address, eventHandler);
    }

    protected EventBusBridge unregisterHandlerInternal(String address, DefaultHandler<?> eventHandler) {
        List<DefaultHandler<?>> handlers = this.handlers.getOrDefault(address, Collections.emptyList());
        handlers.remove(eventHandler);
        if (handlers.isEmpty()) {
            String unregisterMsg = new JsonObject().put("type","unregister").put("address", address).toString();
            webSocket.write(Buffer.buffer(unregisterMsg));
        }
        return this;
    }

    public void close() {
        if (webSocket != null) {
            webSocket.close();
            webSocket = null;
        }
    }

    private void sendMessage(String sendOrPublish, String address, Object message, DefaultHandler<?> replyHandler) {
        JsonObject msg = new JsonObject().put("type", sendOrPublish).put("address", address).put("body", message);
        if (replyHandler != null) {
            String replyAddress = UUID.randomUUID().toString();
            replyHandlers.put(replyAddress, replyHandler);
            msg.put("replyAddress", replyAddress);
        }
        webSocket.write(Buffer.buffer(msg.toString()));
    }

    protected void bufferReceived(Buffer buffer) {
        //System.out.println("Buffer Received");
        //System.out.println(buffer.toString());
        JsonObject msg = new JsonObject(buffer.toString());
        String type = msg.getString("type");
        if ("err".equals(type)) {
            // TODO invoke error handler
            System.err.println("Error message from the event bus bridge:" + msg.toString());
            return;
        }
        String address = msg.getString("address");

        EventBusMessage result = new EventBusMessage(msg);
        for (DefaultHandler<?> h : handlers.getOrDefault(address, Collections.emptyList())) {
            result.deliverTo(h);
        }
        if (replyHandlers.containsKey(address)) {
            DefaultHandler<?> replyHandler = replyHandlers.remove(address);
            result.deliverTo(replyHandler);
        }
    }

    public boolean isOpen() {
        return webSocket != null;
    }

    public class EventBusMessage<T> implements Message<T> {
        String address;
        String replyAddress;
        T body;
        DefaultHandler<T> handler;

        EventBusMessage(JsonObject json) {
            address = json.getString("address");
            replyAddress = json.getString("replyAddress", null);
            body = (T)json.getValue("body");
        }

        EventBusMessage(Message<T> result) {
            address = result.address();
            replyAddress = result.replyAddress();
            body  = result.body();
        }

        @Override
        public String address() {
            return null;
        }

        @Override
        public MultiMap headers() {
            throw new UnsupportedOperationException("No headers supported");
        }

        @Override
        public T body() {
            return body;
        }

        @Override
        public String replyAddress() {
            return replyAddress;
        }

        @Override
        public void reply(Object message) {
            this.reply(message, new DeliveryOptions(), null);
        }

        @Override
        public <R> void reply(Object message, Handler<AsyncResult<Message<R>>> replyHandler) {
            this.reply(message, new DeliveryOptions(), replyHandler);
        }

        @Override
        public void reply(Object message, DeliveryOptions options) {
            this.reply(message, options, null);
        }

        @Override
        public <R> void reply(Object message, DeliveryOptions deliveryOptions, Handler<AsyncResult<Message<R>>> replyHandler) {
            if (this.replyAddress != null) {
                EventBusBridge.this.send(replyAddress, message.toString(), (result,eb) -> {
                    if (replyHandler != null) {
                        replyHandler.handle(new AsyncResult<Message<R>>() {
                            @Override
                            public Message<R> result() {
                            return new EventBusMessage(result);
                        }

                            @Override
                            public Throwable cause() {
                            return null;
                        }

                            @Override
                            public boolean succeeded() {
                            return true;
                        }

                            @Override
                            public boolean failed() {
                            return false;
                        }
                        });
                    }
                });
            }
        }

        @Override
        public void fail(int i, String s) {
            throw new UnsupportedOperationException("Failure is not an option");
        }

        public void unregister() {
            if (handler != null) {
                handler.unregister(address, EventBusBridge.this);
            }
        }

        private void deliverTo(DefaultHandler<T> handler) {
            this.handler = handler; // give handler a chance to un-register
            handler.invoke(this, EventBusBridge.this);
        }

        public Message<JsonObject> asJson() {
            return (Message<JsonObject>) this;
        }
    }

}