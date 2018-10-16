package com.saffrontech.vertx;

import example.ExampleServer;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.*;

public class EventBusBridgeTest {
    static Vertx vertx;
    LongAdder counter = new LongAdder();
    EventBusBridge bridge;

    @BeforeClass
    public static void createServer() throws InterruptedException {
        new ExampleServer().createServer();
    }

    @AfterClass
    public static void stopServer() {
        vertx.close();
    }

    @After
    public void cleanUp() {
        if (bridge != null)
            bridge.close();
    }

    @Test
    public void testConnect() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            eb.close();
            System.out.println("Closing");
            latch.countDown();
        });
        latch.await();
    }

    @Test
    public void testSend() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            eb.registerHandler("test", msg -> {
                assertNotNull(msg);
                latch.countDown();
            });
            eb.send("test", "hello");
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testPublish() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        LongAdder adder = new LongAdder();
        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            registerHandlerInPublish(latch, adder, eb);
            eb.publish("test", "hello");
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(adder.longValue(), 2);
    }

    static void registerHandlerInPublish(CountDownLatch latch, LongAdder adder, EventBusBridge eb) {
        eb.registerHandler("test", msg -> {
            assertNotNull(msg);
            adder.increment();
            latch.countDown();
        }).registerHandler("test", msg -> {
            assertNotNull(msg);
            adder.increment();
            latch.countDown();
        });
    }

    @Test
    public void testSendWithReply() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LongAdder adder = new LongAdder();

        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            eb.registerHandler("replyTest", msg -> {
                msg.reply("bubu");
                adder.increment();
            });
            eb.registerHandler("test", msg -> {
                assertEquals("ok", msg.body().toString());
                latch.countDown();
            });
            eb.send("reply", "to me", reply -> {
                assertEquals("ok", reply.body().toString());
                adder.increment();
            });
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, adder.longValue());
    }

    @Test
    public void testSendWithReply2() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LongAdder adder = new LongAdder();

        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            eb.registerHandler("replyTest", msg -> {
                msg.reply("bubu", reply -> {
                    reply.result().reply("roger");
                    adder.increment();
                });
                adder.increment();
            });
            eb.registerHandler("test", msg -> {
                assertEquals("ok", msg.body().toString());
                latch.countDown();
            });
            eb.send("reply","to me", reply -> {
                assertEquals("ok", reply.body().toString());
                adder.increment();
            });
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(3, adder.longValue());
    }


    @Test
    public void testPublishJson() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        LongAdder adder = new LongAdder();
        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            registerHandlerInPublishJson(latch, adder, eb);
            eb.publish("test", new JsonObject().put("hello", "world"));
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(adder.longValue(), 2);
    }

    static void registerHandlerInPublishJson(CountDownLatch latch, LongAdder adder, EventBusBridge eb) {
        eb.registerHandler("test", msg -> {
            assertNotNull(msg);
            assertEquals("world", msg.asJson().body().getString("hello"));
            adder.increment();
            latch.countDown();
        }).registerHandler("test", (EventBusBridge.EventBusMessage<JsonObject> msg) -> {
            assertNotNull(msg);
            assertEquals("world", msg.body().getString("hello"));
            adder.increment();
            latch.countDown();
        });
    }


    @Test
    public void testRegisterHandler() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        LongAdder adder = new LongAdder();

        MessageHandler mh = msg -> {
            System.out.println("Msg:" + adder.longValue());
            adder.increment();
            latch.countDown();
        };
        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            eb.registerHandler("test", mh);
            eb.registerHandler("test", mh);
            eb.registerHandler("test", mh);
            eb.publish("test", new JsonObject().put("hello", "world"));
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(adder.longValue(), 3);
    }

    @Test
    public void testUnregisterHandler() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            System.out.println("EventBusBridgeTest.testUnregisterHandler");
            EventHandler<?> handleHello = this::handleHello;
            eb.registerHandler("test", handleHello);
            eb.registerHandler("end", msg -> {handleHello.unregister("test",eb); latch.countDown();});
            eb.send("test", "hello");
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, counter.longValue());
    }

    public void handleHello(Message<?> message, EventBusBridge eb) {
        System.out.println("EventBusBridgeTest.handleHello");
        assertNotNull(message);
        if (counter.longValue() == 0) {
            eb.send("end", "test");
        }
        eb.send("test", "hello again"); // this should not be seen by handleHello
        counter.increment();
    }

    @Test
    public void testUnregisterSelf() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            registerHandlerInUnregisterSelf(latch, eb);
            eb.send("test", "hello");
            eb.send("test", "second hello"); // should not be called

        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    static void registerHandlerInUnregisterSelf(CountDownLatch latch, EventBusBridge eb) {
        eb.registerHandler("test", msg -> {
            System.out.println("EventBusBridgeTest.testUnregisterSelf");
            assertEquals("hello", msg.body());
            msg.unregister();
            eb.registerHandler("test", msg2 -> {
                latch.countDown();
            });
        });
    }

    @Test
    public void testHandlers() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);

        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            eb.registerHandler("test", msg -> {
                System.out.println(msg);
                assertNotNull(msg);
                latch.countDown();
            });
            eb.registerHandler("test", (msg, bus) -> { assertEquals(eb, bus); latch.countDown(); });
            eb.publish("test", "hello");
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testClose() throws Exception {
        bridge = EventBusBridge.connect(URI.create("http://localhost:8765/bridge"), eb -> {
            eb.close();
            assertFalse(eb.isOpen());
        });
    }
}