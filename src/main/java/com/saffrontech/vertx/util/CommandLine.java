package com.saffrontech.vertx.util;

import com.saffrontech.vertx.EventBusBridge;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;

/**
 * Created by beders on 7/17/15.
 */
public class CommandLine {
    Scanner scanner;
    PrintStream out;
    boolean interactive;
    Map<String, Consumer<String>> commands = new HashMap<>();
    EventBusBridge bridge;

    CommandLine(InputStream is, OutputStream os) {
        scanner = new Scanner(is);
        out = new PrintStream(os);
        interactive = System.console() != null;
        on("help", line -> {
            out.println("The following commands are available:");
            out.println("connect <uri>\t\t Connect to SockJS service at given address");

        });
        on("connect", line -> {
            URI uri = URI.create(line.trim());
            bridge = EventBusBridge.connect(uri, eb -> {
                out.println("Connected to " + uri);
            });
        });
        on("send", line-> {
            String[] args = line.split(" ");
            String address = args[0];
            String message = args[1];

        });
    }

    private void on(String firstToken, Consumer<String> consumer) {
        commands.put(firstToken, consumer);
    }

    public static void main(String... args) {
        new CommandLine(System.in, System.out).go();
    }

    void go() {
        boolean end = false;
        if (interactive) {
            out.println("Event Bus Command Line. Welcome, user!\nEnter 'help' for usage");
        }
        do {
            try {
                if (interactive) out.print("> ");
                String cmd = scanner.next();
                String rest = scanner.nextLine();

                if (cmd.matches("end|quit|exit")) {
                    end = true;
                } else {
                    commands.getOrDefault(cmd, line -> out.println("Unknown command. Enter 'help' for valid commands")).accept(rest);
                }
            } catch (Exception e) {
                out.println("Oops! " + e.getMessage());
            }
        } while (!end);
        System.exit(0);
    }

}
