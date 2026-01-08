# dataops-prefect-pipeline

This project demonstrates a simple DataOps pipeline using Prefect to ingest, process, and schedule daily weather data, simulating patterns commonly used in energy and forecasting systems.

## Deployment

Flows are deployed directly from GitHub using Prefect Cloud.
The entrypoint references the file path, while internal imports
use the packaged namespace.

## Scheduling & Monitoring

The pipeline runs daily via a cron-based Prefect schedule.
Failure notifications are handled via Prefect Cloud Automations
(email alerts).
