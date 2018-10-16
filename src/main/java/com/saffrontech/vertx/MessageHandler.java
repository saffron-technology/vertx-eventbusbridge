package com.saffrontech.vertx;

public interface MessageHandler<T> extends DefaultHandler<T> {

    void handle(EventBusBridge.EventBusMessage<T> message);

    default void invoke(EventBusBridge.EventBusMessage<T> message, EventBusBridge eb) {
        handle(message);
    }
}
