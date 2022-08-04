from db.mongo.config import Settings
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase


s = Settings()

_MONGO_DETAILS = f'mongodb://{s.MONGO_USERNAME}:{s.MONGO_PASSWORD}@{s.MONGO_HOST}'
_client = AsyncIOMotorClient(_MONGO_DETAILS)
IndagoSession: AsyncIOMotorDatabase = _client.indago