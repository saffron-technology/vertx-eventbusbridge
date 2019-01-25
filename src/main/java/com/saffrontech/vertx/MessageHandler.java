package com.saffrontech.vertx;

import java.io.IOException;

public interface MessageHandler<T> extends DefaultHandler<T> {

    void handle(EventBusBridge.EventBusMessage<T> message) throws IOException;

    default void invoke(EventBusBridge.EventBusMessage<T> message, EventBusBridge eb) {
        try {
            handle(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
