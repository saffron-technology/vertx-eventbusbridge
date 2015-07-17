package com.saffrontech.vertx;

import io.vertx.core.eventbus.Message;

/**
 * Event Handler similar to the standard Vert.x Handler, but also gives access to the event bus bridge
 * Created by beders on 7/6/15.
 */

@FunctionalInterface
public interface EventHandler<T> extends DefaultHandler<T> {
    void handle(EventBusBridge.EventBusMessage<T> message, EventBusBridge bridge);

    default void invoke(EventBusBridge.EventBusMessage<T> message, EventBusBridge eb) {
        handle(message, eb);
    }
}
