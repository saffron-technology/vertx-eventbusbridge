package com.saffrontech.vertx;

import io.vertx.core.eventbus.Message;

/**
 * Created by beders on 7/6/15.
 */
public interface MessageHandler<T> extends DefaultHandler<T> {
    void handle(EventBusBridge.EventBusMessage<T> message);

    default void invoke(EventBusBridge.EventBusMessage<T> message, EventBusBridge eb) {
        handle(message);
    }
}
