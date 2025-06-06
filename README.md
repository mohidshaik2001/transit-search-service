# Transit Search Service

**Realtime vehicle‐tracking API + front-end** for a synthetic city transit system (e.g. Delhi).  
Built with **FastAPI**, **Elasticsearch**, **BigQuery**, **Cloud Run**, **Cloud Functions**, and a **Leaflet** map.

## Overview

This project ingests GTFS schedules, synthetic GPS pings, and incident data (via Cloud Functions),  
merges them in BigQuery (integratorFn), and indexes the most recent 6 hours of data into Elasticsearch (esIndexerFn).  
A **search API** (FastAPI on Cloud Run) exposes queries (route filter, delay, bounding‐box, time range).  
A static **Leaflet front-end** (HTML/JS) fetches `/search` and plots each vehicle’s last ping on a map.

## Architecture

1. **GTFS Processor (Cloud Function)**: Daily builds route_bounds.csv and gtfs_summary.csv.  
2. **GPS Publisher (Cloud Function)**: Hourly synthetic vehicle pings → Pub/Sub.  
3. **Incident Generator & Processor (Cloud Functions)**: Every 15 min write news/tweets; hourly parse → BigQuery `real_time.incidents`.  
4. **Integrator (Cloud Function)**: Every 15 min → BigQuery SQL joins pings, schedules, incidents → `real_time.integrated`.  
5. **esIndexerFn (Cloud Function)**: Every 15 min reads last 6 hours from BigQuery → indexes into Elasticsearch.  
6. **Search API (FastAPI on Cloud Run)**: Filters by route, delay, incidents, bbox, time range.  
7. **Front-end (Leaflet + JS)**: Hosted on GCS (or via FastAPI static mount) → visualizes vehicles on a map.

## Getting Started

### Prerequisites
- GCP project with BigQuery, Cloud Functions, Cloud Run, Secret Manager, Pub/Sub, Elasticsearch (managed or Elastic Cloud)  
- Python 3.10, Docker, gcloud CLI, Git  

### Setup & Deployment

1. **Clone this repo**  
   ```bash
   git clone https://github.com/YourUsername/transit-search-service.git
   cd transit-search-service
