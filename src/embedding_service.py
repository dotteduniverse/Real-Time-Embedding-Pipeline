from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
import numpy as np
from src.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

app = FastAPI(title="Embedding Service")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

class SimilarRequest(BaseModel):
    user_id: str
    top_k: int = 5

@app.get("/embedding/{user_id}")
async def get_embedding(user_id: str):
    key = f"user_embedding:{user_id}"
    val = r.get(key)
    if not val:
        raise HTTPException(status_code=404, detail="User not found")
    embedding = json.loads(val)
    return {"user_id": user_id, "embedding": embedding}

@app.post("/similar")
async def find_similar(req: SimilarRequest):
    # Get query embedding
    query_key = f"user_embedding:{req.user_id}"
    query_val = r.get(query_key)
    if not query_val:
        raise HTTPException(status_code=404, detail="Query user not found")
    query_emb = np.array(json.loads(query_val))

    # For simplicity, get all embeddings and compute cosine similarity (not scalable)
    # In production, use RedisVL or a proper vector index
    keys = r.keys("user_embedding:*")
    similarities = []
    for key in keys:
        uid = key.split(":")[1]
        if uid == req.user_id:
            continue
        emb = np.array(json.loads(r.get(key)))
        sim = np.dot(query_emb, emb) / (np.linalg.norm(query_emb) * np.linalg.norm(emb))
        similarities.append((uid, sim))
    similarities.sort(key=lambda x: x[1], reverse=True)
    top = [uid for uid, _ in similarities[:req.top_k]]
    return {"similar_users": top}