from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, Any
from zipfile import ZipFile
import logging
from datetime import datetime
from ..utils.exceptions import (
    TweetRepositoryError,
    FileNotFoundError,
    InvalidFileFormatError,
    JsonParsingError
)
from .models import Tweet

logger = logging.getLogger(__name__)

class TweetRepository(ABC):
    """Interfaz base para repositorios de tweets."""
    
    @abstractmethod
    async def get_tweets(self) -> AsyncIterator[Tweet]:
        """Retorna un iterador asíncrono de tweets."""
        pass

class JsonParser:
    """Clase utilitaria para parsear JSON de tweets."""

    @staticmethod
    def parse_date(date_str: str) -> datetime:
        """Parsea una fecha del formato ISO a datetime."""
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError as error:
            logger.warning(f"Error parsing date: {date_str}, using current datetime")
            return datetime.utcnow()

    @staticmethod
    def create_tweet(data: Dict[str, Any]) -> Tweet:
        """Crea un objeto Tweet a partir de un diccionario de datos."""
        try:
            tweet = Tweet(**data)
            tweet.mentions = tweet.extract_mentions()
            return tweet
        except Exception as error:
            raise JsonParsingError(f"Error creating tweet: {error}")

class JsonTweetRepository(TweetRepository):
    """Implementación del repositorio que lee tweets desde un archivo JSON."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.parser = JsonParser()
        logger.info(f"Inicializando repositorio con archivo: {file_path}")

    def _validate_file(self) -> None:
        """Valida que el archivo tenga el formato correcto."""
        if not self.file_path.endswith('.zip'):
            raise InvalidFileFormatError("El archivo debe ser un ZIP")

    async def _process_json_stream(self, json_file) -> AsyncIterator[Tweet]:
        """Procesa el stream de JSON y genera tweets."""
        current_obj: Dict[str, Any] = {}
        tweets_processed = 0

        try:
            import ijson
            parser = ijson.parse(json_file)

            for prefix, event, value in parser:
                if prefix.endswith('.user_name'):
                    current_obj['user_name'] = value
                elif prefix.endswith('.created_at'):
                    current_obj['created_at'] = self.parser.parse_date(value)
                elif prefix.endswith('.text'):
                    current_obj['text'] = value
                    tweets_processed += 1
                    
                    if tweets_processed % 1000 == 0:
                        logger.info(f"Procesados {tweets_processed} tweets")

                    try:
                        yield self.parser.create_tweet(current_obj)
                    finally:
                        current_obj = {}

        except ijson.JSONError as error:
            raise JsonParsingError(f"Error parsing JSON: {error}")

    async def get_tweets(self) -> AsyncIterator[Tweet]:
        """
        Lee tweets desde un archivo JSON comprimido usando streaming asíncrono.
        
        Yields:
            Tweet: Objetos Tweet uno a la vez
            
        Raises:
            FileNotFoundError: Si el archivo no existe
            InvalidFileFormatError: Si el formato del archivo no es válido
            JsonParsingError: Si hay errores al parsear el JSON
            TweetRepositoryError: Para otros errores inesperados
        """
        self._validate_file()

        try:
            with ZipFile(self.file_path) as zip_file:
                json_filename = zip_file.namelist()[0]
                logger.info(f"Procesando archivo JSON: {json_filename}")

                with zip_file.open(json_filename) as json_file:
                    async for tweet in self._process_json_stream(json_file):
                        yield tweet

        except FileNotFoundError as error:
            raise FileNotFoundError(f"Archivo no encontrado: {self.file_path}")
        except Exception as error:
            raise TweetRepositoryError(f"Error inesperado: {error}")

    async def __aenter__(self):
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        pass