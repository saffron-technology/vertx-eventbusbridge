package example;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * Created by beders on 7/17/15.
 */
public class ExampleServer {
    Vertx vertx;

    public void createServer() throws InterruptedException {
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
        vertx.createHttpServer().requestHandler(router::accept).listen(8765, (res) -> {
            latch.countDown();
        });

        latch.await();
        System.out.println("Server listening on port 8765");
    }

    public static void main(String... args) throws InterruptedException {
        new ExampleServer().createServer();
    }
}
