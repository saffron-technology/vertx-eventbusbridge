package com.saffrontech.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.*;

/**
 * Created by beders on 6/25/15.
 */
public class EventBusBridgeSSLTest {
    static Vertx vertx;
    LongAdder counter = new LongAdder();
    EventBusBridge bridge;

    @BeforeClass
    public static void createServer() throws InterruptedException {
        vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);
        Router router = Router.router(vertx);

        // events specific to THOPs are made available over the bridge
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
        BridgeOptions options = new BridgeOptions();
        options.addOutboundPermitted(new PermittedOptions().setAddress("test")).
                addInboundPermitted(new PermittedOptions().setAddress("test")).
                addOutboundPermitted(new PermittedOptions().setAddress("end")).
                addInboundPermitted(new PermittedOptions().setAddress("end")).
                addInboundPermitted(new PermittedOptions().setAddress("reply")).
                addOutboundPermitted(new PermittedOptions().setAddress("replyTest"));

        sockJSHandler.bridge(options);

        router.route("/bridge/*").handler(sockJSHandler);
        // for reply test
        vertx.eventBus().consumer("reply", msg -> {
            vertx.eventBus().send("replyTest", "replyToMe", reply -> {
                assertEquals("bubu", reply.result().body().toString());
                reply.result().reply("ok", replyOfreply -> {
                    assertEquals("roger", replyOfreply.result().body().toString());
                });
                msg.reply("ok");
                vertx.eventBus().send("test", "ok");
            });
        });
        vertx.createHttpServer(new HttpServerOptions().setSsl(true).setKeyStoreOptions(new JksOptions().setPath("keystore.jks").setPassword("bubulala"))).requestHandler(router::accept).listen(8765, (res) -> {
            latch.countDown();
        });

        latch.await();
        System.out.println("SSL Server listening on port 8765");
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

        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
            eb.close();
            System.out.println("Closing");
            latch.countDown();
        }, localSSLOptions());
        latch.await();
    }

    private static HttpClientOptions localSSLOptions() {
        return new HttpClientOptions().setSsl(true).setTrustAll(true).setVerifyHost(false);
    }

    @Test
    public void testSend() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
            eb.registerHandler("test", msg -> {
                assertNotNull(msg);
                latch.countDown();
            });
            eb.send("test", "hello");
        },localSSLOptions());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testPublish() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        LongAdder adder = new LongAdder();
        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
            eb.registerHandler("test", msg -> {
                assertNotNull(msg);
                adder.increment();
                latch.countDown();
            }).registerHandler("test", msg -> {
                assertNotNull(msg);
                adder.increment();
                latch.countDown();
            });
            eb.publish("test", "hello");
        }, localSSLOptions());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(adder.longValue(), 2);
    }

    @Test
    public void testSendWithReply() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LongAdder adder = new LongAdder();

        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
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
        }, localSSLOptions());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, adder.longValue());
    }

    @Test
    public void testSendWithReply2() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LongAdder adder = new LongAdder();

        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
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
        }, localSSLOptions());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(3, adder.longValue());
    }


    @Test
    public void testPublishJson() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        LongAdder adder = new LongAdder();
        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
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
            eb.publish("test", new JsonObject().put("hello", "world"));
        }, localSSLOptions());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(adder.longValue(), 2);
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
        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
            eb.registerHandler("test", mh);
            eb.registerHandler("test", mh);
            eb.registerHandler("test", mh);
            eb.publish("test", new JsonObject().put("hello", "world"));
        }, localSSLOptions());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(adder.longValue(), 3);
    }

    @Test
    public void testUnregisterHandler() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
            System.out.println("EventBusBridgeTest.testUnregisterHandler");
            EventHandler<?> handleHello = this::handleHello;
            eb.registerHandler("test", handleHello);
            eb.registerHandler("end", msg -> {handleHello.unregister("test",eb); latch.countDown();});
            eb.send("test", "hello");
        }, localSSLOptions());
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

        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
            eb.registerHandler("test", msg -> {
                System.out.println("EventBusBridgeTest.testUnregisterSelf");
                assertEquals("hello", msg.body());
                msg.unregister();
                eb.registerHandler("test", msg2 -> {
                    latch.countDown();
                });
            });
            eb.send("test", "hello");
            eb.send("test", "second hello"); // should not be called

        }, localSSLOptions());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
    @Test
    public void testHandlers() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);

        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
            eb.registerHandler("test", msg -> {
                System.out.println(msg);
                assertNotNull(msg);
                latch.countDown();
            });
            eb.registerHandler("test", (msg, bus) -> { assertEquals(eb, bus); latch.countDown(); });
            eb.publish("test", "hello");
        }, localSSLOptions());
        assertTrue(latch.await(5, TimeUnit.SECONDS));

    }

    @Test
    public void testClose() throws Exception {
        bridge = EventBusBridge.connect(URI.create("https://localhost:8765/bridge"), eb -> {
            eb.close();
            assertFalse(eb.isOpen());
        }, localSSLOptions());
    }
}