from typing import List, Tuple, Iterator
import json
import logging
from zipfile import ZipFile, BadZipFile
from memory_profiler import profile
from collections import Counter
import re
from src.utils.exceptions import TweetRepositoryError

logger = logging.getLogger(__name__)

class MentionTracker:
    """
    Clase para rastrear y gestionar menciones de manera eficiente en memoria.
    Mantiene un contador de menciones y realiza limpiezas periódicas.
    """
    def __init__(self, max_mentions: int = 100, cleanup_frequency: int = 10000):
        self.mention_counts = Counter()
        self.max_mentions = max_mentions
        self.cleanup_frequency = cleanup_frequency
        self.tweets_processed = 0
        self.mentions_found = 0
        self._pattern = re.compile(r'@([a-zA-Z0-9_]+)')
        
    def process_tweet(self, text: str) -> None:
        """
        Procesa un tweet para extraer y contar menciones.
        Realiza limpieza periódica según la frecuencia configurada.
        """
        if not isinstance(text, str):
            return
            
        # Extraer menciones usando regex compilado
        mentions = self._pattern.findall(text)
        
        # Actualizar contadores
        self.mention_counts.update(mentions)
        self.mentions_found += len(mentions)
        self.tweets_processed += 1
        
        # Realizar limpieza periódica si es necesario
        if self._should_cleanup():
            self._cleanup_memory()
            
    def _should_cleanup(self) -> bool:
        """Determina si es momento de realizar limpieza de memoria."""
        return (self.tweets_processed % self.cleanup_frequency == 0 and 
                len(self.mention_counts) > self.max_mentions)
        
    def _cleanup_memory(self) -> None:
        """
        Realiza limpieza de memoria manteniendo solo las menciones más frecuentes.
        Registra la operación en el log.
        """
        before_size = len(self.mention_counts)
        self.mention_counts = Counter(dict(
            self.mention_counts.most_common(self.max_mentions)
        ))
        after_size = len(self.mention_counts)
        
        logger.info(
            "Limpieza de memoria - Menciones: %d → %d | Tweets: %d | Total menciones: %d",
            before_size, after_size, self.tweets_processed, self.mentions_found
        )
        
    def get_top_mentions(self, n: int = 10) -> List[Tuple[str, int]]:
        """Retorna las n menciones más frecuentes."""
        return self.mention_counts.most_common(n)
        
    @property
    def stats(self) -> dict:
        """Retorna estadísticas del procesamiento."""
        return {
            'tweets_processed': self.tweets_processed,
            'mentions_found': self.mentions_found,
            'unique_mentions': len(self.mention_counts)
        }

def process_tweets(file_path: str) -> Iterator[str]:
    """Lee tweets del archivo ZIP línea por línea."""
    try:
        with ZipFile(file_path) as zip_file:
            try:
                json_filename = zip_file.namelist()[0]
            except IndexError:
                logger.error("El archivo ZIP %s está vacío", file_path)
                raise TweetRepositoryError("El archivo ZIP está vacío")
                
            with zip_file.open(json_filename) as json_file:
                for line_number, line in enumerate(json_file, 1):
                    try:
                        tweet = json.loads(line)
                        if content := tweet.get('content'):
                            yield content
                    except (json.JSONDecodeError, Exception) as error:
                        logger.warning("Error en línea %d: %s", line_number, error)
                        continue

    except (FileNotFoundError, BadZipFile) as error:
        logger.error("Error con el archivo %s: %s", file_path, error)
        raise
    except Exception as error:
        logger.error("Error procesando archivo: %s", error)
        raise TweetRepositoryError(f"Error procesando archivo: {error}")

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los 10 usuarios más mencionados.
    Implementación optimizada para memoria usando MentionTracker.
    
    Args:
        file_path: Ruta al archivo ZIP con los tweets
        
    Returns:
        Lista de tuplas (username, conteo_menciones) con los 10 usuarios más mencionados
    """
    try:
        # Inicializar tracker con configuración por defecto
        tracker = MentionTracker()
        
        # Procesar tweets uno por uno
        for text in process_tweets(file_path):
            tracker.process_tweet(text)
            
        # Verificar si se encontraron menciones
        if tracker.stats['mentions_found'] == 0:
            logger.warning("No se encontraron menciones")
            return []
        
        # Registrar estadísticas finales
        stats = tracker.stats
        logger.info(
            "Análisis completado: %d tweets procesados, %d menciones totales, %d menciones únicas",
            stats['tweets_processed'], stats['mentions_found'], stats['unique_mentions']
        )
        
        return tracker.get_top_mentions()
        
    except Exception as error:
        logger.error("Error en el análisis: %s", error)
        raise TweetRepositoryError(f"Error en el análisis: {error}")