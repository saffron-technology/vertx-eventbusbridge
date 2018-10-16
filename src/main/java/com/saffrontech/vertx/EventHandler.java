package com.saffrontech.vertx;

/**
 * Event Handler similar to the standard Vert.x Handler, but also gives access to the event bus bridge
 */

@FunctionalInterface
public interface EventHandler<T> extends DefaultHandler<T> {

    void handle(EventBusBridge.EventBusMessage<T> message, EventBusBridge bridge);

    default void invoke(EventBusBridge.EventBusMessage<T> message, EventBusBridge eb) {
        handle(message, eb);
    }
}
