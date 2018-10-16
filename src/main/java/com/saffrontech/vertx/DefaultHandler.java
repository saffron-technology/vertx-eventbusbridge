package com.saffrontech.vertx;

/**
 * Base interface for handlers. Use EventHandler or Handler instead.
 */
public interface DefaultHandler<T> {

    default void invoke(EventBusBridge.EventBusMessage<T> message, EventBusBridge eb) {

        throw new IllegalStateException("Use EventHandler or MessageHandler instead of this interface");
    }

    default void unregister(String address, EventBusBridge eb) {
        eb.unregisterHandlerInternal(address, this);
    }
}
