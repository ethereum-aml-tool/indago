import os
from pydantic import BaseSettings

class Settings(BaseSettings):
    # MONGODB
    MONGO_USERNAME: str
    MONGO_PASSWORD: str
    MONGO_HOST: str

# print(Settings().dict())