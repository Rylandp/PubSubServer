import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

//============================================================================
// pbs - Pub-Sub Broker Server
//
// An HTTP/1.1 pub-sub message broker.
//
// Endpoints:
//   POST /subscribe/<topic>       Subscribe client to a topic
//   POST /unsubscribe/<topic>     Unsubscribe client from a topic
//   POST /publish/<topic>         Publish message body to all subscribers
//   GET  /messages                Retrieve and clear pending messages
//   GET  /status                  List topics with subscriber counts
//
// Client identification: X-Client-ID header (required for all except /status).
//
// Usage: java pbs.java [-q] [port]
//
//============================================================================

public class pbs {
    private static final int DEFAULT_PORT = 8080;

    // Topic -> set of subscribed client IDs
    private final ConcurrentHashMap<String, Set<String>> topics = new ConcurrentHashMap<>();

    // Client ID -> queue of pending messages
    private final ConcurrentHashMap<String, Queue<String>> mailboxes = new ConcurrentHashMap<>();

    private boolean quiet = false;

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // main / appMain — entry point and server startup
    //
    // Parses command-line arguments and enters the accept loop.
    // Each connection is handled in a virtual thread.
    //
    // DO NOT MODIFY main or appMain.
    //

    public static void main(String[] args) {
        new pbs().appMain(args);
    }

    public void appMain(String[] args) {
        int port = DEFAULT_PORT;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-q")) {
                quiet = true;
            } else {
                try {
                    port = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port, using default: " + DEFAULT_PORT);
                }
            }
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("PBS started on port " + port);

            while (true) {
                try {
                    Socket client = serverSocket.accept();
                    Thread.ofVirtual().start(() -> {
                        try {
                            handleConnection(client);
                        } catch (IOException e) {
                            // Client disconnected or error
                        } finally {
                            try { client.close(); } catch (IOException e) { }
                        }
                    });
                } catch (IOException e) {
                    System.err.println("Accept error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not start server: " + e.getMessage());
        }
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Connection Handling
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // handleConnection
    //
    // Called once per connection. You must:
    //   1. Create a BufferedReader and OutputStream from the socket
    //   2. Read the request line (e.g., "GET /status HTTP/1.1")
    //   3. Parse headers using parseHeaders()
    //   4. If the request is a POST with a Content-Length header,
    //      read that many characters from the reader as the body
    //   5. Extract the HTTP method and path from the request line
    //   6. Extract the X-Client-ID from the headers (may be null)
    //   7. Call dispatchRequest() with method, path, clientId, and body
    //
    // Hint: Split the request line on spaces to get [method, path, version].
    // Hint: Use parseHeaders() — it is provided for you.
    //
    private void handleConnection(Socket socket) throws IOException {
        // TODO: Implement
        BufferedReader in = new BufferedReader(
            new InputStreamReader(socket.getInputStream()));

        OutputStream out = socket.getOutputStream();
        
        String line = in.readLine();
        while(line == null || line.isEmpty()) {
            return;
        }

        ConcurrentHashMap<String, String> headers = new ConcurrentHashMap<>(parseHeaders(in));
        
        String HTTPMethod = line.split(" ")[0];
        String path = line.split(" ")[1];
        String clientID = headers.get("x-client-id");

        String body = null;
        if(HTTPMethod.equals("POST") && headers.containsKey("content-length")) {
            int length = Integer.parseInt(headers.get("content-length"));
            char[] buf = new char[length];
            int read = in.read(buf);
            body = new String(buf, 0, read);
        }
        dispatchRequest(out, HTTPMethod, path, clientID, body);

    }

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // parseHeaders
    //
    // Reads HTTP headers from the input stream and returns them as a map.
    // Header names are normalized to lowercase. Reads until the blank line
    // that separates headers from the body.
    //
    // PROVIDED — do not modify.
    //
    private Map<String, String> parseHeaders(BufferedReader in) throws IOException {
        Map<String, String> headers = new HashMap<>();
        String line;
        while ((line = in.readLine()) != null && !line.isEmpty()) {
            int colon = line.indexOf(':');
            if (colon > 0) {
                headers.put(
                    line.substring(0, colon).trim().toLowerCase(),
                    line.substring(colon + 1).trim()
                );
            }
        }
        return headers;
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Request Routing
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // dispatchRequest
    //
    // Routes the request to the correct handler based on method and path.
    //
    // Routing table:
    //   GET  /status              -> handleStatus(out)
    //   GET  /messages            -> handleMessages(out, clientId)
    //   POST /subscribe/<topic>   -> handleSubscribe(out, clientId, topic)
    //   POST /unsubscribe/<topic> -> handleUnsubscribe(out, clientId, topic)
    //   POST /publish/<topic>     -> handlePublish(out, clientId, topic, body)
    //   anything else             -> sendError(out, 404, "Not Found")
    //
    // For endpoints that require X-Client-ID, send a 400 error if it is null.
    // Extract the topic from the path by removing the prefix
    //   (e.g., "/subscribe/weather" -> "weather").
    //
    private void dispatchRequest(OutputStream out, String method, String path,
                                  String clientId, String body) throws IOException {
        // TODO: Implement
        
        if(method.equals("GET")) {
            if(path.equals("/status")) {
                handleStatus(out);
            } else if (path.equals("/messages")) {
                if(clientId == null) {
                    sendError(out, 400, "Missing X-Client-ID");
                } else {
                    handleMessages(out, clientId);
                }
            } else {
                sendError(out, 404, "Not Found");
            }
        } else if (method.equals("POST")) {
            String[] parts = path.split("/");
            if (parts.length < 3) {
                sendError(out, 404, "Not Found");
                return;
            }
            String action = parts[1];
            String topic = parts[2];
            if(clientId == null) {
                sendError(out, 400, "Missing X-Client-ID");
                return;
            }
            if(action.equals("subscribe")) {
                handleSubscribe(out, clientId, topic);
            } else if(action.equals("unsubscribe")) {
                handleUnsubscribe(out, clientId, topic);
            } else if(action.equals("publish")) {
                handlePublish(out, clientId, topic, body);
            } else {
                sendError(out, 404, "Not Found");
            }
        } else {
            sendError(out, 404, "Not Found");
        }
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Endpoint Handlers
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // handleSubscribe
    //
    // Adds clientId to the subscriber set for the given topic.
    // Creates the topic set and client mailbox if they don't exist.
    //
    // Response JSON: {"status":"subscribed","topic":"<topic>"}
    //
    // Hint: topics.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet())
    // Hint: mailboxes.computeIfAbsent(clientId, k -> new ConcurrentLinkedQueue<>())
    //
    private void handleSubscribe(OutputStream out, String clientId, String topic) throws IOException {
        // TODO: Implement
        Set<String> subscribers = topics.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet());
        subscribers.add(clientId);

        mailboxes.computeIfAbsent(clientId, k -> new ConcurrentLinkedQueue<>());

        String json = "{\"status\":\"subscribed\",\"topic\":\"" + topic + "\"}";
        byte[] body = json.getBytes(StandardCharsets.UTF_8);

        sendResponse(out, 200, "OK", "application/json", body);
    }

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // handleUnsubscribe
    //
    // Removes clientId from the subscriber set for the given topic.
    // If the topic has no remaining subscribers, remove it from the map.
    //
    // Response JSON: {"status":"unsubscribed","topic":"<topic>"}
    //
    private void handleUnsubscribe(OutputStream out, String clientId, String topic) throws IOException {
        // TODO: Implement
        Set<String> subscribers = topics.get(topic);
        if(subscribers != null) {
            subscribers.remove(clientId);
            if(subscribers.isEmpty()){
                topics.remove(topic);
            }
        }
        

        String json = "{\"status\":\"unsubscribed\",\"topic\":\"" + topic + "\"}";
        byte[] body = json.getBytes(StandardCharsets.UTF_8);

        sendResponse(out, 200, "OK", "application/json", body);

    }

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // handlePublish
    //
    // Delivers the message body to all subscribers of the given topic.
    // For each subscriber, add "<topic>: <body>" to their mailbox queue.
    // Count how many subscribers received the message.
    //
    // Response JSON: {"status":"published","topic":"<topic>","delivered":<n>}
    //
    private void handlePublish(OutputStream out, String clientId, String topic, String body) throws IOException {
        // TODO: Implement
        Set<String> subscribers = topics.get(topic);
        int delivered = 0;
        
        if (subscribers != null) {
            for (String subscriberId : subscribers) {
                Queue<String> mailbox = mailboxes.get(subscriberId);
                mailbox.add(topic + ": " + body);
                delivered++;
            }
        }
        
        String json = "{\"status\":\"published\",\"topic\":\"" + topic + "\",\"delivered\":" + delivered + "}";
        byte[] bodyBytes = json.getBytes(StandardCharsets.UTF_8);
        sendResponse(out, 200, "OK", "application/json", bodyBytes);
    }

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // handleMessages
    //
    // Drains the client's mailbox and returns all pending messages.
    // After this call the mailbox should be empty.
    //
    // Response JSON: {"messages":["msg1","msg2",...],"count":<n>}
    //
    // Hint: Use queue.poll() in a loop until it returns null.
    // Hint: Use escapeJson() on each message string.
    //
    private void handleMessages(OutputStream out, String clientId) throws IOException {
        // TODO: Implement
        Queue<String> mailbox = mailboxes.get(clientId);
        List<String> messages = new ArrayList<>();
        
        if (mailbox != null) {
            String msg;
            while ((msg = mailbox.poll()) != null) {
                messages.add(escapeJson(msg));
            }
        }
        
        StringBuilder json = new StringBuilder("{\"messages\":[");
        for (int i = 0; i < messages.size(); i++) {
            if (i > 0) json.append(",");
            json.append("\"").append(messages.get(i)).append("\"");
        }
        json.append("],\"count\":").append(messages.size()).append("}");
        
        byte[] bodyBytes = json.toString().getBytes(StandardCharsets.UTF_8);
        sendResponse(out, 200, "OK", "application/json", bodyBytes);
    }

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // handleStatus
    //
    // Returns all topics and their subscriber counts.
    //
    // Response JSON: {"topics":{"weather":3,"sports":1}}
    //
    private void handleStatus(OutputStream out) throws IOException {
        // TODO: Implement
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // HTTP Response Helpers
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // sendResponse
    //
    // Sends a complete HTTP/1.1 response: status line, headers, blank
    // line, and body.
    //
    // Required headers:
    //   Content-Type: <contentType>
    //   Content-Length: <body.length>
    //   Connection: close
    //
    // CRITICAL: Use \r\n (CRLF) for line endings, not \n.
    // CRITICAL: There must be a blank line (\r\n) between headers and body.
    //
    // Hint: Use PrintWriter for the status line and headers (don't forget
    //       to flush it), then write body bytes directly to the OutputStream.
    //
    private void sendResponse(OutputStream out, int code, String message,
                               String contentType, byte[] body) throws IOException {
        // TODO: Implement
        PrintWriter writer = new PrintWriter(out, true);
        writer.print("HTTP/1.1 " + code + " " + message + "\r\n");
        writer.print("Content-Type: " + contentType + "\r\n");
        writer.print("Content-Length: " + body.length + "\r\n");
        writer.print("Connection: close:\r\n");
        writer.print("\r\n");
        writer.flush();

        out.write(body);
        out.flush();
    }

    //''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    // sendError
    //
    // Sends an error response with a JSON body: {"error":"<message>"}
    //
    // This is a convenience wrapper around sendResponse.
    //
    private void sendError(OutputStream out, int code, String message) throws IOException {
        // TODO: Implement
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Utilities — PROVIDED, do not modify
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
