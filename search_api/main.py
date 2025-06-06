import os
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from elasticsearch import Elasticsearch, ElasticsearchException
from typing import Optional, List, Dict
from pydantic import BaseModel, Field
import traceback
from google.cloud import secretmanager

def access_secret(secret_name: str) -> str:
    """
    Reads the latest version of the given secret from Secret Manager.
    Expects that the Cloud Run service account has "secretmanager.secretAccessor" on each secret.
    """
    project_id = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise RuntimeError("GCP_PROJECT (or GOOGLE_CLOUD_PROJECT) must be set")

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


# ------------------------------------------------------------------------------
# 1) Fetch ES_ENDPOINT and ES_API_KEY from Secret Manager on cold start
# ------------------------------------------------------------------------------

ES_ENDPOINT = access_secret("elastic-endpoint")
ES_API_KEY   = access_secret("elastic-api-key")

# DEBUG: Uncomment to verify the values in Cloud Run logs
# print(f"[DEBUG] ES_ENDPOINT={ES_ENDPOINT!r}")
# print(f"[DEBUG] ES_API_KEY={'<redacted>' if ES_API_KEY else None}")

# ------------------------------------------------------------------------------
# 2) Initialize Elasticsearch client
# ------------------------------------------------------------------------------

try:
    es = Elasticsearch([ES_ENDPOINT], api_key=ES_API_KEY)
except Exception:
    print("[DEBUG] Elasticsearch client init failed:")
    traceback.print_exc()
    es = None

# ------------------------------------------------------------------------------
# 3) FastAPI setup and Pydantic models
# ------------------------------------------------------------------------------

app = FastAPI(
    title="Transit Search Service",
    description="Search for transit vehicle pings with various filters.",
    version="1.0.0"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # in production, replace "*" with ["https://storage.googleapis.com"]
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)



# 3) Pydantic models
class Hit(BaseModel):
    vehicle_id: str
    ping_ts: str
    stop_id: str
    schedu_ts: str
    delay_sec: int
    location: Dict[str, float] = Field(
        ..., 
        example={"lat": 12.916, "lon": 77.599}
    )
    incident_count: int

class SearchResponse(BaseModel):
    total: int
    results: List[Hit]

# ------------------------------------------------------------------------------
# 4) /search endpoint
# ------------------------------------------------------------------------------

@app.get("/search", response_model=SearchResponse)
async def search(
    route_id: Optional[str] = Query(None, description="Route number (e.g. 2 → matches vehicle_id '2.0_*')"),
    min_delay: Optional[int] = Query(None, ge=0),
    min_incidents: Optional[int] = Query(None, ge=0),
    bbox: Optional[str] = Query(None),
    time_from: Optional[str] = Query(None),
    time_to: Optional[str] = Query(None),
    size: int = Query(25, ge=1, le=100)
):
    try:
        must_clauses = []
        filter_clauses = []

        # 1) Route filter via prefix on vehicle_id (e.g. "2.0_")
        if route_id:
            prefix_val = f"{route_id}.0_"
            must_clauses.append({"prefix": {"vehicle_id": prefix_val}})

        # 2) Delay filter
        if min_delay is not None:
            must_clauses.append({"range": {"delay_sec": {"gte": min_delay}}})

        # 3) Incident filter
        if min_incidents is not None:
            must_clauses.append({"range": {"incident_count": {"gte": min_incidents}}})

        # 4) Time-range filter
        if time_from or time_to:
            ts_range = {}
            if time_from:
                ts_range["gte"] = time_from
            if time_to:
                ts_range["lte"] = time_to
            must_clauses.append({"range": {"ping_ts": ts_range}})

        # 5) Geobounding box filter
        if bbox:
            try:
                lat1, lon1, lat2, lon2 = map(float, bbox.split(","))
                filter_clauses.append({
                    "geo_bounding_box": {
                        "location": {
                            "top_left":     {"lat": lat2, "lon": lon1},
                            "bottom_right": {"lat": lat1, "lon": lon2}
                        }
                    }
                })
            except Exception:
                raise HTTPException(status_code=400, detail="`bbox` must be four comma-separated floats: lat1,lon1,lat2,lon2")

        # If no must_clauses provided, match all
        if not must_clauses:
            must_clauses = [{"match_all": {}}]

        query_body = {
            "query": {
                "bool": {
                    "must":   must_clauses,
                    "filter": filter_clauses
                }
            },
            "size": size,
            "_source": [
                "vehicle_id",
                "ping_ts",
                "stop_id",
                "schedu_ts",
                "delay_sec",
                "location",
                "incident_count"
            ]
        }

        # Execute search
        try:
            resp = es.search(index="transit-integrated", body=query_body)
        except ElasticsearchException as e:
            raise HTTPException(status_code=500, detail=f"Elasticsearch query failed: {str(e)}")

        hits = [hit["_source"] for hit in resp["hits"]["hits"]]
        total = resp["hits"]["total"]["value"]
        return {"total": total, "results": hits}

    except HTTPException:
        # Re‐raise HTTPExceptions (400/500) directly
        raise
    except Exception as e:
        # Print full traceback for Cloud Run logs, then return 500
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
