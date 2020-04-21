import asyncio
import os

from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

client = AsyncIOMotorClient(os.getenv('SCRAPY_SETTING_MONGO_URI'))
db = client.get_default_database()


async def update_fan_page_from_fb_user():
    cursor = db.fb_user.find(
        {
            'action_path': {
                '$ne': "/a/mobile/friends/add_friend.php",
                '$exists': 1
            }
        }
    )

    futures = [
        db.fan_page.update_one(
            {'page_id': user['uid']},
            {
                '$set': {
                    'link': 'https://www.facebook.com' + user['profile_link']
                }
            },
            upsert=True
        )
        async for user in cursor
    ]

    results = await asyncio.gather(*futures)
    newline = '\n'
    print(f"{newline.join([str(result.raw_result) for result in results])}\n{'='*20}\n{datetime.now()}\n\n")
    return results

async def main_loop():
    print('main loop start\n')
    while True:
        await asyncio.gather(
            update_fan_page_from_fb_user(),
            asyncio.sleep(3600)
        )


loop = asyncio.get_event_loop()
loop.run_until_complete(main_loop())
