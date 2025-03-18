# tsen-smartwatch-health-data-simulation

This service simulates the streaming of smartwatch data to a server. It supports sending events for a pre-generated dataset of devices, adjusting the speed of event sending (relative to real-time), and a fast mode for testing by sending events as quickly as possible.

## Features
- **Ordered requests per device**: Events for each device are sent sequentially, ensuring that the order is preserved.
- **Speed factor**: Control how fast the events are sent relative to their original timestamps.
- **Fast mode**: Send all events as fast as possible without delays but still in the correct order.

## Requirements
- Docker

## Running the Service

### Build the Docker Image

```bash
	docker build -t streaming-client .
```

### Run the Docker Container

#### Parameters

- `--server_url`: The URL of the server where the events will be sent.
- `--speed`: The speed factor for sending events (e.g. 1.0 for real-time, 10.0 for 10x faster).
- `--fast_mode`: Use this flag to send events as fast as possible without delays.

#### Examples

##### MacOS / Windows

```bash
	docker run -it --rm streaming-client --server_url http://host.docker.internal:62333/stream --speed 10.0
```

For fast mode:

```bash
	docker run -it --rm streaming-client --server_url http://host.docker.internal:62333/stream --fast_mode
```



##### Linux

```bash
	docker run -it --rm --network="host" streaming-client --server_url http://localhost:62333/stream --speed 10.0
```

For fast mode:

```bash
	docker run -it --rm --network="host" streaming-client --server_url http://localhost:62333/stream --fast_mode
```



## Notes
- Make sure your server is running and ready to accept requests at the specified `--server_url`.
- Adjust the speed factor or use fast mode based on your testing requirements.