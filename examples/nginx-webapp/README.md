# Nginx + Web Application Example

This example demonstrates managing multiple processes: a Python web application and Nginx as a reverse proxy, all running inside a Docker container.

## Architecture

```
Client -> Docker Container
              |
              +-> Cirond (PID 1)
                    |
                    +-> Nginx (port 80) -> Python App (port 8080)
```

## Running with Docker

1. Build the Docker image (from project root):
```bash
docker build -f examples/nginx-webapp/Dockerfile -t ciron-nginx-example .
```

2. Run the container:
```bash
docker run --rm -p 8080:80 -p 50051:50051 ciron-nginx-example
```

3. Test the application:
```bash
# Access through Nginx
curl http://localhost:8080

# Health check
curl http://localhost:8080/health
```

4. Control processes from host:
```bash
# Build cironctl if not already built
cargo build --release --bin cironctl

# Check status
./target/release/cironctl -t inet://127.0.0.1:50051 status

# Stop nginx
./target/release/cironctl -t inet://127.0.0.1:50051 stop nginx

# Restart webapp
./target/release/cironctl -t inet://127.0.0.1:50051 restart webapp
```

## What's Happening

- **cirond**: Runs as PID 1 in the container, managing both processes
- **webapp**: Python HTTP server on port 8080
- **nginx**: Nginx reverse proxy on port 80, forwarding to Python app
- `nginx` declares `after = ["webapp"]` and `wants = ["webapp"]`, so cirond starts
  webapp first
- Both processes automatically restart if they crash
- You can control processes via **cironctl** from outside the container
