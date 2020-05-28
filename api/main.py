import os
from datetime import datetime

from fastapi import FastAPI, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from starlette.responses import RedirectResponse, Response

client = AsyncIOMotorClient(os.getenv('MONGO_URI'))
db = client.get_default_database()

app = FastAPI(on_shutdown=[lambda: client.close()])


class Post(BaseModel):
    page_id: int
    posted_time: datetime
    msg: str


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


@app.get("/page/{page_id}")
async def read_item(page_id: int, limit: int = 10):
    cursor = db.post.find({"page_id": page_id})
    if limit:
        cursor = cursor.limit(limit)
    return [Post(**post) async for post in cursor]
