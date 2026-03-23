"""Optional: initialize Redis index using RedisVL for vector search."""
import redis
from redisvl.index import SearchIndex
from redisvl.schema import IndexSchema

# Example of creating a vector index (requires redisvl)
# Not mandatory for MVP, but can be added for production-like setup.

def init_redis_index():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    schema = IndexSchema.from_dict({
        "index": {
            "name": "user_embeddings",
            "prefix": "user_embedding:",
        },
        "fields": [
            {"name": "user_id", "type": "tag"},
            {"name": "embedding", "type": "vector", "attrs": {"dims": 768, "algorithm": "flat"}}
        ]
    })
    index = SearchIndex(schema)
    index.create(r)

if __name__ == "__main__":
    init_redis_index()