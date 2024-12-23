from typing import Iterator
import json
from zipfile import ZipFile
from .tweet_repository import TweetRepository
from .models import Tweet

class MemoryOptimizedRepository(TweetRepository):
    """Implementación optimizada para memoria del repositorio."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path

    def get_tweets(self) -> Iterator[Tweet]:
        """Lee tweets uno a uno para optimizar memoria."""
        with ZipFile(self.file_path) as zip_file:
            json_filename = zip_file.namelist()[0]
            with zip_file.open(json_filename) as json_file:
                for line in json_file:
                    try:
                        tweet_data = json.loads(line.decode('utf-8'))
                        # Transformar los datos al formato esperado
                        processed_data = {
                            'created_at': tweet_data.get('date'),
                            'user_name': tweet_data.get('user', {}),
                            'text': tweet_data.get('content', '')
                        }
                        yield Tweet(**processed_data)
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"Error procesando tweet: {e}")
                        continue