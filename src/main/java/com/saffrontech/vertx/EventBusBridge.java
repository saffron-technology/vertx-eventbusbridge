package com.saffrontech.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
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
 */
public class EventBusBridge {

    private Vertx vertx;
    private WebSocket webSocket;
    private long pingTimerID;
    private ConcurrentHashMap<String, List<DefaultHandler<?>>> handlers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, DefaultHandler<?>> replyHandlers = new ConcurrentHashMap<>();

    private static final int MAX_SOCKET_FRAME_SIZE = 2 * (int) Math.pow(2, 18); // 512K max payload

    /**
     * Create an event bus bridge using an absolute URL and default socket frame size (512K).
     */
    public static EventBusBridge connect(URI endPoint,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler) {
        return connect(-1, null, endPoint, onOpenHandler, null, null);
    }

    public static EventBusBridge connect(URI endPoint,
                                         MultiMap headers,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler) {
        return connect(-1, null, endPoint, headers, onOpenHandler, null, null);
    }

    /**
     * Create an event bus bridge using an absolute or relative URL with additional options.
     * Note: If a relative URL is provided, defaultPort and defaultHost from options will be used!
     *
     * @param endPoint
     * @param onOpenHandler
     * @param options       http options. If you are connecting through SSL to a server with a self-signed certificate, use setTrustAll and verifyHost options.
     * @return
     */
    public static EventBusBridge connect(URI endPoint,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                                         HttpClientOptions options) {
        return connect(-1, null, endPoint, onOpenHandler, options, null);
    }

    /**
     * Provide your own Vertx instance.
     *
     * @see EventBusBridge#connect(URI, Handler)
     */

    public static EventBusBridge connect(URI endPoint,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                                         Vertx vertx) {
        return connect(-1, null, endPoint, onOpenHandler, null, vertx);
    }

    /**
     * Use this static method to connect through a proxy.
     *
     * @param port          port to use
     * @param host          the host to connect to
     * @param endPoint      the actual endpoint. Use an absolute URL when connecting through a proxy.
     * @param onOpenHandler
     * @param vertx         a vertx instance (optional)
     * @return
     */
    public static EventBusBridge connect(int port,
                                         String host,
                                         URI endPoint,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                                         Vertx vertx) {
        return connect(port, host, endPoint, onOpenHandler, null, vertx);
    }

    /**
     * Use this static method to connect through a proxy.
     *
     * @param port          port to use
     * @param host          the host to connect to
     * @param endPoint      the actual endpoint. Use an absolute URL when connecting through a proxy.
     * @param onOpenHandler
     * @param options
     * @return
     */
    public static EventBusBridge connect(int port,
                                         String host,
                                         URI endPoint,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                                         HttpClientOptions options) {
        return connect(port, host, endPoint, onOpenHandler, options, null);
    }

    public static EventBusBridge connect(URI endPoint,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                                         int maxSocketFrameSize) {
        return connect(endPoint, onOpenHandler, maxSocketFrameSize, null);
    }

    public static EventBusBridge connect(URI endPoint,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                                         int maxSocketFrameSize,
                                         Vertx vertx) {
        return connect(-1, null, endPoint, onOpenHandler, new HttpClientOptions().setMaxWebsocketFrameSize(maxSocketFrameSize), vertx);
    }

    public static EventBusBridge connect(int port,
                                         String host,
                                         URI endPoint,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                                         HttpClientOptions options,
                                         Vertx vertx) {
        HttpClientOptions actualOptions = options == null ? new HttpClientOptions().setMaxWebsocketFrameSize(MAX_SOCKET_FRAME_SIZE) : options;
        int actualPort = guessPort(port, endPoint, actualOptions);
        String actualHost = guessHost(host, endPoint, actualOptions);
        actualOptions.setSsl(guessSsl(endPoint, options));
        return new EventBusBridge(actualPort, actualHost, endPoint, onOpenHandler, actualOptions, Optional.ofNullable(vertx));
    }

    public static EventBusBridge connect(int port,
                                         String host,
                                         URI endPoint,
                                         MultiMap headers,
                                         io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                                         HttpClientOptions options,
                                         Vertx vertx) {
        HttpClientOptions actualOptions = options == null ? new HttpClientOptions().setMaxWebsocketFrameSize(MAX_SOCKET_FRAME_SIZE) : options;
        int actualPort = guessPort(port, endPoint, actualOptions);
        String actualHost = guessHost(host, endPoint, actualOptions);
        actualOptions.setSsl(guessSsl(endPoint, options));
        return new EventBusBridge(actualPort, actualHost, endPoint, headers, onOpenHandler, actualOptions, Optional.ofNullable(vertx));
    }

    /**
     * Guess port: It is either set explicitly, taken from the absolute URL or taken from the default options
     */
    private static int guessPort(int port,
                                 URI endPoint,
                                 HttpClientOptions options) {
        if (port != -1) return port;
        if (endPoint.isAbsolute()) {
            if (endPoint.getPort() == -1) {
                return portFromScheme(endPoint.getScheme());
            } else {
                return endPoint.getPort();
            }
        } else {
            return options.getDefaultPort();
        }
    }

    /**
     * Guess host:  It is either set explicitly, taken from the absolute URL or taken from the default options
     */
    private static String guessHost(String host,
                                    URI endPoint,
                                    HttpClientOptions actualOptions) {
        if (host == null || host.isEmpty()) {
            if (endPoint.isAbsolute()) {
                return endPoint.getHost();
            } else {
                return actualOptions.getDefaultHost();
            }
        } else {
            return host;
        }
    }

    /**
     * Guess SSL: Either taken from scheme or set directly in options
     */
    private static boolean guessSsl(URI endPoint,
                                    HttpClientOptions options) {
        if (endPoint.isAbsolute()) {
            return endPoint.getScheme().equals("https");
        } else {
            return options.isSsl();
        }
    }


    private static int portFromScheme(String scheme) {
        return scheme.equals("https") ? 443 : 80;
    }

    private EventBusBridge(int port,
                           String host,
                           URI endPoint,
                           io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                           HttpClientOptions options,
                           Optional<Vertx> aVertx) {
        vertx = aVertx.orElse(Vertx.vertx());
        vertx.createHttpClient(options).websocket(port, host, endPoint.toString() + "/websocket", ws -> {
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

    private EventBusBridge(int port,
                           String host,
                           URI endPoint,
                           MultiMap headers,
                           io.vertx.core.Handler<EventBusBridge> onOpenHandler,
                           HttpClientOptions options,
                           Optional<Vertx> aVertx) {
        vertx = aVertx.orElse(Vertx.vertx());
        vertx.createHttpClient(options).websocket(port, host, endPoint.toString() + "/websocket", headers, ws -> {
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

    public EventBusBridge send(String address,
                               String message) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, null);
        return this;
    }

    public EventBusBridge publish(String address,
                                  String message) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, null);
        return this;
    }

    public EventBusBridge send(String address,
                               String message,
                               EventHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, replyHandler);
        return this;
    }

    public EventBusBridge publish(String address,
                                  String message,
                                  EventHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, replyHandler);
        return this;
    }

    public EventBusBridge send(String address,
                               String message,
                               MessageHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, replyHandler);
        return this;
    }

    public EventBusBridge publish(String address,
                                  String message,
                                  MessageHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, replyHandler);
        return this;
    }

    public EventBusBridge send(String address,
                               JsonObject message) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, null);
        return this;
    }

    public EventBusBridge publish(String address,
                                  JsonObject message) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, null);
        return this;
    }

    public EventBusBridge send(String address,
                               JsonObject message,
                               EventHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, replyHandler);
        return this;
    }

    public EventBusBridge publish(String address,
                                  JsonObject message,
                                  EventHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, replyHandler);
        return this;
    }

    public EventBusBridge send(String address,
                               JsonObject message,
                               MessageHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("send", address, message, replyHandler);
        return this;
    }

    public EventBusBridge publish(String address,
                                  JsonObject message,
                                  MessageHandler<?> replyHandler) {
        Objects.requireNonNull(webSocket);
        sendMessage("publish", address, message, replyHandler);
        return this;
    }

    EventBusBridge registerHandler(String address,
                                   MessageHandler<?> messageHandler) {
        return registerHandlerInternal(address, messageHandler);
    }

    EventBusBridge registerHandler(String address,
                                   EventHandler<?> eventHandler) {
        return registerHandlerInternal(address, eventHandler);
    }

    private EventBusBridge registerHandlerInternal(String address,
                                                   DefaultHandler<?> eventHandler) {
        handlers.computeIfAbsent(address, key -> {
            webSocket.write(Buffer.buffer(new JsonObject().put("type", "register").put("address", address).toString()));
            return Collections.synchronizedList(new ArrayList<>());
        }).add(eventHandler);
        return this;
    }

    public EventBusBridge unregisterHandler(String address,
                                            MessageHandler<?> messageHandler) {
        return unregisterHandlerInternal(address, messageHandler);
    }

    public EventBusBridge unregisterHandler(String address,
                                            EventHandler<?> eventHandler) {
        return unregisterHandlerInternal(address, eventHandler);
    }

    EventBusBridge unregisterHandlerInternal(String address,
                                             DefaultHandler<?> eventHandler) {
        List<DefaultHandler<?>> handlers = this.handlers.getOrDefault(address, Collections.emptyList());
        handlers.remove(eventHandler);
        if (handlers.isEmpty()) {
            String unregisterMsg = new JsonObject().put("type", "unregister").put("address", address).toString();
            webSocket.write(Buffer.buffer(unregisterMsg));
        }
        return this;
    }

    public void close() {
        if (webSocket != null) {
            webSocket.close();
            webSocket = null;
        }
        vertx.close();
    }

    private void sendMessage(String sendOrPublish,
                             String address,
                             Object message,
                             DefaultHandler<?> replyHandler) {
        JsonObject msg = new JsonObject().put("type", sendOrPublish).put("address", address).put("body", message);
        if (replyHandler != null) {
            String replyAddress = UUID.randomUUID().toString();
            replyHandlers.put(replyAddress, replyHandler);
            msg.put("replyAddress", replyAddress);
        }
        webSocket.write(Buffer.buffer(msg.toString()));
    }

    private void bufferReceived(Buffer buffer) {
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
            body = (T) json.getValue("body");
        }

        EventBusMessage(Message<T> result) {
            address = result.address();
            replyAddress = result.replyAddress();
            body = result.body();
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
                EventBusBridge.this.send(replyAddress, message.toString(), (result, eb) -> {
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

        void unregister() {
            if (handler != null) {
                handler.unregister(address, EventBusBridge.this);
            }
        }

        private void deliverTo(DefaultHandler<T> handler) {
            this.handler = handler; // give handler a chance to un-register
            handler.invoke(this, EventBusBridge.this);
        }

        Message<JsonObject> asJson() {
            return (Message<JsonObject>) this;
        }
    }

}