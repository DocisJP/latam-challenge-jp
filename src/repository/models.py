from pydantic import BaseModel, Field
from datetime import datetime

class Tweet(BaseModel):
    created_at: datetime
    username: str = Field(alias='user_name')
    text: str