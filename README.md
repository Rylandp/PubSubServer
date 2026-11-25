# Pub-Sub Broker (`pbs` & `pbc`)

A lightweight HTTP/1.1 publish-subscribe message broker (`pbs.java`) and automated end-to-end test client (`pbc.java`) built in Java using raw TCP sockets and Java virtual threads.

## Architecture & Features

- **`pbs.java` (Pub-Sub Broker Server)**:
  - Listens on a configurable TCP port (default `8080`) and spawns a virtual thread (`Thread.ofVirtual()`) per incoming HTTP/1.1 connection.
  - Manages topic subscriptions via a thread-safe `ConcurrentHashMap<String, Set<String>>` and per-client message queues via `ConcurrentHashMap<String, Queue<String>>` (`ConcurrentLinkedQueue`).
  - Automatically prunes empty topics when the last subscriber unsubscribes and drains client mailboxes upon retrieval (`GET /messages`).
- **`pbc.java` (Pub-Sub Broker Client / Test Tool)**:
  - Runs an automated multi-phase integration test across configurable topics, subscribers per topic, cross-topic subscribers (`sub-multi`), and message counts.
  - Verifies subscription registration, `/status` topic counts, message fan-out delivery, mailbox clearing after polling, and post-unsubscribe isolation.

## Requirements

- **Java 21+** (required for `Thread.ofVirtual()` virtual threads and running single-file `.java` programs directly).

## Running the Project

### 1. Start the Broker Server

```bash
java pbs.java [-q] [port]
```

Example (start on default port `8080`):

```bash
java pbs.java 8080
```

### 2. Run the Automated Test Client

```bash
java pbc.java [options]
```

| Flag | Description | Default |
| :--- | :--- | :--- |
| `-h`, `--host <host>` | Server hostname | `localhost` |
| `-p`, `--port <port>` | Server port | `8080` |
| `-t`, `--topics <n>` | Number of topics (`topic-0` .. `topic-(n-1)`) | `3` |
| `-s`, `--subscribers <n>` | Subscribers per topic | `2` |
| `-m`, `--messages <n>` | Messages published per topic | `5` |
| `-d`, `--delay <ms>` | Delay in milliseconds between publishes | `50` |
| `--help` | Print CLI usage information | — |

Example (5 topics, 3 subscribers per topic, 10 messages per topic, 10 ms delay):

```bash
java pbc.java -h localhost -p 8080 -t 5 -s 3 -m 10 -d 10
```

## HTTP/1.1 API Reference

All endpoints except `GET /status` require the `X-Client-ID` request header (missing headers return `400` with `{"error":"Missing X-Client-ID"}`).

| Method | Endpoint | Headers | Request Body | Response JSON (`200 OK`) |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/subscribe/<topic>` | `X-Client-ID: <id>` | *(empty)* | `{"status":"subscribed","topic":"<topic>"}` |
| `POST` | `/unsubscribe/<topic>` | `X-Client-ID: <id>` | *(empty)* | `{"status":"unsubscribed","topic":"<topic>"}` |
| `POST` | `/publish/<topic>` | `X-Client-ID: <id>` | Raw message text | `{"status":"published","topic":"<topic>","delivered":<n>}` |
| `GET` | `/messages` | `X-Client-ID: <id>` | *(none)* | `{"messages":["<topic>: <msg>",...],"count":<n>}` |
| `GET` | `/status` | *(optional)* | *(none)* | `{"topics":{"<topic>":<subscriber_count>,...}}` |

### Manual `curl` Example

```bash
# Subscribe client-1 to "weather"
curl -X POST http://localhost:8080/subscribe/weather -H "X-Client-ID: client-1"

# Publish a message to "weather"
curl -X POST http://localhost:8080/publish/weather -H "X-Client-ID: pub-1" -d "72F and sunny"

# Poll and drain messages for client-1
curl http://localhost:8080/messages -H "X-Client-ID: client-1"

# Check active topics and subscriber counts
curl http://localhost:8080/status
```

## Project Structure

```text
.
├── pbs.java    # HTTP/1.1 Pub-Sub Broker Server (virtual threads, ConcurrentHashMap state)
└── pbc.java    # Configurable multi-phase Pub-Sub test client and HTTP
