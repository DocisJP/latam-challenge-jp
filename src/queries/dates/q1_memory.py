from typing import List, Tuple, Iterator
from datetime import datetime
import json
import logging
from zipfile import ZipFile
from collections import Counter
from memory_profiler import profile
from src.utils.exceptions import TweetRepositoryError

logger = logging.getLogger(__name__)

def process_tweets(file_path: str) -> Iterator[tuple]:
    """
    Lee tweets del archivo ZIP línea por línea para minimizar uso de memoria.
    
    Args:
        file_path: Ruta al archivo ZIP con los tweets
        
    Yields:
        Tupla de (fecha, username)
    """
    try:
        with ZipFile(file_path) as zip_file:
            json_filename = zip_file.namelist()[0]
            with zip_file.open(json_filename) as json_file:
                for line_number, line in enumerate(json_file, 1):
                    try:
                        tweet = json.loads(line)
                        date = datetime.fromisoformat(
                            tweet['date'].replace('Z', '+00:00')
                        ).date()
                        username = tweet['user']['username']
                        yield (date, username)
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logger.warning("Error en línea %s: %s", line_number, str(e))
                        continue

    except Exception as error:
        logger.error("Error procesando archivo: %s", error)
        raise TweetRepositoryError(f"Error al procesar archivo: {error}")

class DateTracker:
    """
    Clase para rastrear estadísticas por fecha de manera eficiente en memoria.
    """
    def __init__(self, max_dates: int = 20):
        self.date_counts = Counter()
        self.current_user_counts = Counter()
        self.top_users = {}
        self.current_date = None
        self.max_dates = max_dates
        self._prune_threshold = max_dates * 2

    def add_tweet(self, date: datetime.date, username: str) -> None:
        """Agrega un tweet al tracking."""
        self.date_counts[date] += 1

        if date != self.current_date:
            if self.current_date and self.current_user_counts:
                # Guardar el usuario más activo de la fecha anterior
                top_user = self.current_user_counts.most_common(1)[0][0]
                if self.date_counts[self.current_date] >= self._get_min_count():
                    self.top_users[self.current_date] = top_user
            self.current_user_counts.clear()
            self.current_date = date

        self.current_user_counts[username] += 1
        
        # Podar datos cuando sea necesario
        if len(self.date_counts) > self._prune_threshold:
            self._prune_data()

    def _get_min_count(self) -> int:
        """Obtiene el conteo mínimo entre las top fechas."""
        if len(self.date_counts) < self.max_dates:
            return 0
        return min(count for _, count in self.date_counts.most_common(self.max_dates))

    def _prune_data(self) -> None:
        """Elimina fechas que no pueden estar en el top final."""
        min_count = self._get_min_count()
        self.date_counts = Counter({
            date: count for date, count in self.date_counts.items()
            if count >= min_count
        })
        self.top_users = {
            date: user for date, user in self.top_users.items()
            if self.date_counts[date] >= min_count
        }

    def get_top_results(self, n: int = 10) -> List[Tuple[datetime.date, str]]:
        """Obtiene los resultados finales."""
        # Procesar la última fecha si es necesario
        if self.current_date and self.current_user_counts:
            top_user = self.current_user_counts.most_common(1)[0][0]
            if self.date_counts[self.current_date] >= self._get_min_count():
                self.top_users[self.current_date] = top_user

        # Obtener top fechas y sus usuarios
        top_dates = self.date_counts.most_common(n)
        return [
            (date, self.top_users[date])
            for date, _ in top_dates
            if date in self.top_users
        ]

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Encuentra las 10 fechas con más tweets y el usuario con más publicaciones por fecha.
    Implementación optimizada para memoria usando streaming y estructuras eficientes.
    
    Args:
        file_path: Ruta al archivo ZIP con los tweets
        
    Returns:
        Lista de tuplas (fecha, username) con las 10 fechas con más tweets
    """
    try:
        tracker = DateTracker()
        tweets_processed = 0
        
        logger.info("Iniciando procesamiento de tweets")
        
        for tweet_date, username in process_tweets(file_path):
            tracker.add_tweet(tweet_date, username)
            tweets_processed += 1
            
            if tweets_processed % 10000 == 0:
                logger.info("Procesados %s tweets", tweets_processed)

        logger.info("Procesamiento completado: %d tweets analizados", tweets_processed)
        return tracker.get_top_results()
        
    except Exception as error:
        logger.error("Error en el análisis: %s", error)
        raise TweetRepositoryError(f"Error en el análisis: {error}")