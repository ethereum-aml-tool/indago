import os
from pydantic import BaseSettings

# TODO Move to make more global?
class Settings(BaseSettings):
    # SQL Database
    
    # LOCALHOST/DOCKER
    DB_TYPE: str = "postgresql"
    POSTGRES_USERNAME: str = "postgres"
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str = "db"
    POSTGRES_PORT: str = "5432"
    POSTGRES_NAME: str = "postgres"
    
    # AWS
    if os.getenv("DB_HOST") == "AWS":
        DB_TYPE: str = "postgresql"
        POSTGRES_USERNAME: str = "dbmasteruser"
        POSTGRES_PASSWORD: str
        POSTGRES_HOST: str = "ls-4a0a2add25ae69fc6b996b70138a86270d8ad91c.ce55q3wzm6bh.eu-north-1.rds.amazonaws.com"
        POSTGRES_PORT: str = "5432"
        POSTGRES_NAME: str = "postgres"
    
    # API-keys
    ETHERSCAN_API_KEY: str = "NONE"

# print(Settings().dict())