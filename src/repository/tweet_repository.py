from abc import ABC, abstractmethod
from typing import Iterator
from .models import Tweet

class TweetRepository(ABC):
    @abstractmethod
    def get_tweets(self) -> Iterator[Tweet]:
        """Retorna un iterador de tweets."""
        pass