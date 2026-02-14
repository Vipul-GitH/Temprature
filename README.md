# Room Temperature Dashboard

This repository hosts a FastAPI-based dashboard that monitors temperature and humidity traces across labs. A Docker image is already described by `Dockerfile`, so you can build, run, and push the container to any registry that you control.

### Prerequisites
- Docker installed locally (docker daemon running)
- Connection details for the MySQL instance that holds the sensor data
- [Optional] Docker Hub or other registry credentials for pushing

### Build the Docker image
1. Copy `env.example` to `.env` and update the values so the app can reach your database (`MYSQL_HOST`, `MYSQL_PASSWORD`, etc.).
2. From the project root:
   ```sh
   docker build -t <your-namespace>/room-temperature-dashboard:latest .
   ```

### Run the container
```sh
docker run --rm -p 3003:3003 \
  --env-file .env \
  <your-namespace>/room-temperature-dashboard:latest
```

The dashboard will be available at `http://localhost:3003/temperature`. The `health` endpoint remains on `/health`.

### Push to a Docker registry
1. Authenticate with your registry (e.g., `docker login` for Docker Hub).
2. Tag the local image:
   ```sh
   docker tag <your-namespace>/room-temperature-dashboard:latest <your-namespace>/room-temperature-dashboard:<tag>
   ```
3. Push:
   ```sh
   docker push <your-namespace>/room-temperature-dashboard:<tag>
   ```

Repeat the tag/push steps if you need multiple release channels (e.g., `latest`, `staging`, `v1.0`).

### Environment variables
Use `.env` (copied from `env.example`) so the container at runtime can connect to both primary and secondary MySQL instances and provide infra ticket metadata.

- `ENABLE_ALERT_SCHEDULER` (default `true`) toggles the background thread that scans rooms and creates infra tickets.
- `ALERT_SCAN_INTERVAL_MINUTES` (default `60`) controls how frequently the scheduler runs the scan.

### Notes
- The Dockerfile installs system-level dependencies required by the MySQL connector and pins Python packages via `requirements.txt`.
- Logs are written to `logs/infra_alerts.log` when the infra router posts notifications.
