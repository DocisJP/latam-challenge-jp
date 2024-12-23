import pytest
import asyncio
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import json
import tempfile
from src.repository.tweet_repository import JsonTweetRepository, JsonParser
from src.repository.models import Tweet
from src.utils.exceptions import (
    TweetRepositoryError,
    FileNotFoundError,
    InvalidFileFormatError,
    JsonParsingError
)

@pytest.fixture
def sample_tweet_data():
    """Fixture que provee datos de ejemplo para un tweet."""
    return {
        "user_name": "test_user",
        "created_at": "2024-03-20T10:00:00Z",
        "text": "Hello @mention1 and @mention2 👋"
    }

@pytest.fixture
def sample_zip_file():
    """Fixture que crea un archivo ZIP temporal con tweets de prueba."""
    tweets = [
        {
            "user_name": f"user_{i}",
            "created_at": "2024-03-20T10:00:00Z",
            "text": f"Tweet {i} from @user_{i+1} 👋"
        }
        for i in range(5)
    ]
    
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_zip:
        with ZipFile(tmp_zip.name, 'w', compression=ZIP_DEFLATED) as zip_file:
            zip_file.writestr('tweets.json', json.dumps(tweets))
        
        yield tmp_zip.name
    
    # Limpieza después de los tests
    Path(tmp_zip.name).unlink(missing_ok=True)

class TestJsonParser:
    """Tests unitarios para JsonParser."""
    
    def test_parse_date_valid(self):
        """Test para parsing de fecha válida."""
        date_str = "2024-03-20T10:00:00Z"
        result = JsonParser.parse_date(date_str)
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 20

    def test_parse_date_invalid(self):
        """Test para parsing de fecha inválida."""
        date_str = "invalid_date"
        result = JsonParser.parse_date(date_str)
        assert isinstance(result, datetime)
        # Debería devolver la fecha actual
        assert result <= datetime.utcnow()

    def test_create_tweet_valid(self, sample_tweet_data):
        """Test para creación de tweet con datos válidos."""
        tweet = JsonParser.create_tweet(sample_tweet_data)
        assert isinstance(tweet, Tweet)
        assert tweet.username == "test_user"
        assert len(tweet.mentions) == 2
        assert "mention1" in tweet.mentions
        assert "mention2" in tweet.mentions

    def test_create_tweet_invalid(self):
        """Test para creación de tweet con datos inválidos."""
        invalid_data = {"invalid": "data"}
        with pytest.raises(JsonParsingError):
            JsonParser.create_tweet(invalid_data)

class TestJsonTweetRepository:
    """Tests de integración para JsonTweetRepository."""
    
    @pytest.mark.asyncio
    async def test_get_tweets_success(self, sample_zip_file):
        """Test para lectura exitosa de tweets."""
        async with JsonTweetRepository(sample_zip_file) as repo:
            tweets = []
            async for tweet in repo.get_tweets():
                tweets.append(tweet)
            
            assert len(tweets) == 5
            assert all(isinstance(tweet, Tweet) for tweet in tweets)
            assert all(tweet.username.startswith("user_") for tweet in tweets)

    @pytest.mark.asyncio
    async def test_get_tweets_invalid_file(self):
        """Test para manejo de archivo inválido."""
        with pytest.raises(FileNotFoundError):
            async with JsonTweetRepository("nonexistent.zip") as repo:
                async for _ in repo.get_tweets():
                    pass

    @pytest.mark.asyncio
    async def test_get_tweets_invalid_format(self, tmp_path):
        """Test para manejo de formato de archivo inválido."""
        invalid_file = tmp_path / "test.txt"
        invalid_file.write_text("")
        
        with pytest.raises(InvalidFileFormatError):
            async with JsonTweetRepository(str(invalid_file)) as repo:
                async for _ in repo.get_tweets():
                    pass

    @pytest.mark.asyncio
    async def test_get_tweets_invalid_json(self, tmp_path):
        """Test para manejo de JSON inválido."""
        zip_path = tmp_path / "invalid.zip"
        with ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('tweets.json', "invalid json")
        
        with pytest.raises(JsonParsingError):
            async with JsonTweetRepository(str(zip_path)) as repo:
                async for _ in repo.get_tweets():
                    pass

    @pytest.mark.asyncio
    async def test_get_tweets_empty_file(self, tmp_path):
        """Test para manejo de archivo vacío."""
        zip_path = tmp_path / "empty.zip"
        with ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('tweets.json', "[]")
        
        async with JsonTweetRepository(str(zip_path)) as repo:
            tweets = [tweet async for tweet in repo.get_tweets()]
            assert len(tweets) == 0