from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class Tweet(BaseModel):
    """Modelo que representa un Tweet con los campos mínimos necesarios."""
    created_at: datetime
    username: str = Field(alias='user_name')
    text: str

    @field_validator('username', mode='before')
    @classmethod
    def extract_username(cls, value):
        """Extrae el username del objeto de usuario si es necesario."""
        if isinstance(value, dict):
            return value.get('username', '')
        return value

    @field_validator('text', mode='before')
    @classmethod
    def extract_text(cls, value):
        """Extrae el texto del tweet."""
        if isinstance(value, dict):
            return value.get('content', '')
        return value

    class Config:
        """Configuración del modelo para optimizar memoria."""
        frozen = True
        extra = 'ignore'

    def __str__(self) -> str:
        return f"Tweet(username={self.username}, created_at={self.created_at.date()})"