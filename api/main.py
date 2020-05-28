import os
from datetime import datetime

from fastapi import FastAPI, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from starlette.responses import RedirectResponse, Response

client = AsyncIOMotorClient(os.getenv('MONGO_URI'))
db = client.get_default_database()

app = FastAPI(on_shutdown=[lambda: client.close()])


class Page(BaseModel):
    page_id: int
    link: str


@app.get("/", status_code=307, response_class=Response)
def read_root():
    return RedirectResponse("/docs")


@app.get("/pages")
async def list_page():
    cursor = db.fan_page.find(projection={"_id": 0})
    return [Page(**page) async for page in cursor]


@app.get("/page/{page_id}/posts")
async def get_posts(page_id: int, limit: int = 10):
    cursor = db.post.find({"page_id": page_id},
                          projection={'_id': 0, 'url_path': 0, 'page_id': 0})
    if limit:
        cursor = cursor.limit(limit)
    return [post async for post in cursor]


@app.get("/post/{post_id}/reactions")
async def get_reactions(post_id: int, limit: int = 1):
    if limit > 10:
        limit = 10
    cursor = db.history.post_reactions.aggregate([
        {'$match': {'post_id': post_id}},
        {'$sort': {'date': -1}},
        {'$project': {'_id': 0}},
        {'$limit': limit},
    ])
    return [doc async for doc in cursor]
