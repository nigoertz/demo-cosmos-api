import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import ObjectId
from pydantic import BaseModel, Field
from typing import Any, List, Optional

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv('MONITORING_URL')],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
cosmosdb_uri = "mongodb+srv://{user}:{password}@{url}/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000".format(
    user=os.getenv('COSMOSDB_USER'),
    password=os.getenv('COSMOSDB_PASSWORD'),
    url=os.getenv('COSMOSDB_URL')
)

try:
    client = MongoClient(cosmosdb_uri)
    db = client.get_database('demodb') 
    snapshot_collection = db.get_collection('snapshots')
    step_collection = db.get_collection('steps')
    transaction_collection = db.get_collection('transactions')
    log_collection = db.get_collection('logs')
except ConnectionFailure:
    raise HTTPException(status_code=500, detail="Failed to connect to Cosmos DB")

# Models
class Node(BaseModel):
    type: str
    name: str

class Message(BaseModel):
    _msgid: str
    payload: Any
    topic: str  
    _firstnode: str
    _previousnode: Optional[str] = None
    _lastnode: Optional[str] = None

class Snapshot(BaseModel):
    id: str
    transaction: str
    node: Node
    createdAt: int
    msg: Message

class Step(BaseModel):
    id: str
    topic: str
    node: Node
    transaction: str
    createdAt: int
    snapshotId: str

class Log(BaseModel):
    id: str

class Transaction(BaseModel):
    id: str
    start: int
    end: int
    receiver: List[str]
    sender: str
    logs: List[Log] = Field(default_factory=list)

# Helper function to convert MongoDB objects to dict
def mongo_to_dict(mongo_obj):
    if isinstance(mongo_obj, ObjectId):
        return str(mongo_obj)
    if isinstance(mongo_obj, dict):
        return {k: mongo_to_dict(v) for k, v in mongo_obj.items()}
    if isinstance(mongo_obj, list):
        return [mongo_to_dict(i) for i in mongo_obj]
    return mongo_obj

max_size_transactions_collection = 3
chunk_size_to_delete_from_transactions_collection = 1

max_size_steps_collection = 50
chunk_size_to_delete_from_steps_collection = 1

max_size_snapshots_collection = 50
chunk_size_to_delete_from_snapshots_collection = 1

max_size_log_collection = 50
chunk_size_to_delete_from_log_collection = 1

# Endpoints
@app.post("/snapshots")
async def create_snapshot(snapshot: Snapshot):
    snapshot_dict = snapshot.dict()
    if snapshot_collection.count_documents({}) >= max_size_snapshots_collection:
        oldest_snapshots = snapshot_collection.find().sort("createdAt", 1).limit(chunk_size_to_delete_from_snapshots_collection)
        ids_to_delete = [entry['_id'] for entry in oldest_snapshots]
        snapshot_collection.delete_many({"_id": {"$in": ids_to_delete}})
    snapshot_collection.insert_one(snapshot_dict)
    return {"message": "Snapshot saved", "snapshot": snapshot}

@app.post("/steps")
async def create_step(step: Step):
    step_dict = step.dict()
    if step_collection.count_documents({}) >= max_size_steps_collection:
        oldest_steps = step_collection.find().sort("createdAt", 1).limit(chunk_size_to_delete_from_steps_collection)
        ids_to_delete = [entry['_id'] for entry in oldest_steps]
        step_collection.delete_many({"_id": {"$in": ids_to_delete}})
    step_collection.insert_one(step_dict)
    return {"message": "Step saved", "step": step}

@app.post("/transactions")
async def create_transaction(transaction: Transaction):
    transaction_dict = transaction.dict()
    if transaction_collection.count_documents({}) >= max_size_transactions_collection:
        oldest_transactions = transaction_collection.find().sort("start", 1).limit(chunk_size_to_delete_from_transactions_collection)
        ids_to_delete = [entry['_id'] for entry in oldest_transactions]
        transaction_collection.delete_many({"_id": {"$in": ids_to_delete}})
    transaction_collection.insert_one(transaction_dict)
    return {"message": "Transaction saved", "transaction": transaction}

@app.post("/logs")
async def create_log(log: Log):
    log_dict = log.dict()
    if log_collection.count_documents({}) >= max_size_log_collection:
        oldest_logs = log_collection.find().sort("_id", 1).limit(chunk_size_to_delete_from_log_collection)
        ids_to_delete = [entry['_id'] for entry in oldest_logs]
        log_collection.delete_many({"_id": {"$in": ids_to_delete}})
    log_collection.insert_one(log_dict)
    return {"message": "Log saved", "log": log}

@app.get("/status")
async def get_status():
    return "Connected to API"

@app.get("/snapshots")
async def get_all_snapshots():
    snapshots = list(snapshot_collection.find())
    return [mongo_to_dict(snapshot) for snapshot in snapshots]

@app.get("/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: str):
    snapshot = snapshot_collection.find_one({"id": snapshot_id})
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return mongo_to_dict(snapshot)

@app.get("/transactions")
async def get_all_transactions(count: int = 10, offset: int = 0):
    transactions = list(transaction_collection.find().skip(offset).limit(count))
    for transaction in transactions:
        steps = list(step_collection.find({"transaction": transaction["id"]}))
        transaction["steps"] = [mongo_to_dict(step) for step in steps]
    return [mongo_to_dict(transaction) for transaction in transactions]

@app.get("/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    transaction = transaction_collection.find_one({"id": transaction_id})
    if transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    steps = list(step_collection.find({"transaction": transaction_id}))
    transaction["steps"] = [mongo_to_dict(step) for step in steps]
    return mongo_to_dict(transaction)

@app.get("/transactions/{transaction_id}/snapshots")
async def get_snapshots_for_transaction(transaction_id: str):
    snapshots = list(snapshot_collection.find({"transaction": transaction_id}))
    if not snapshots:
        raise HTTPException(status_code=404, detail="No snapshots found for this transaction")
    return [mongo_to_dict(snapshot) for snapshot in snapshots]

@app.get("/steps/{step_id}")
async def get_step(step_id: str):
    step = step_collection.find_one({"id": step_id})
    if step is None:
        raise HTTPException(status_code=404, detail="Step not found")
    return mongo_to_dict(step)

@app.get("/logs/{log_id}")
async def get_log(log_id: str):
    log = log_collection.find_one({"id": log_id})
    if log is None:
        raise HTTPException(status_code=404, detail="Log not found")
    return mongo_to_dict(log)

@app.delete("/delete")
async def delete_entries(number_of_entries: int, collection_name: str):
    collections = {
        "snapshots": snapshot_collection,
        "steps": step_collection,
        "transactions": transaction_collection,
        "logs": log_collection,
    }
    
    if collection_name not in collections:
        raise HTTPException(status_code=400, detail="Invalid collection name")

    collection = collections[collection_name]
    entries_to_delete = collection.find().limit(number_of_entries)
    ids_to_delete = [entry['_id'] for entry in entries_to_delete]
    
    delete_result = collection.delete_many({"_id": {"$in": ids_to_delete}})
    return {"message": f"{delete_result.deleted_count} entries deleted from {collection_name} collection"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
