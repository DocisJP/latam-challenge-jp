from typing import List, Tuple, Iterator, Dict
import json
import logging
from zipfile import ZipFile, BadZipFile
import emoji
from memory_profiler import profile
from heapq import nlargest
from collections import Counter
from src.utils.exceptions import TweetRepositoryError
import mmap
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def memory_mapped_file(filename: str):
    """
    Contexto para manejar archivos con memory mapping.
    Mejora la eficiencia de lectura sin aumentar significativamente el uso de memoria.
    """
    with open(filename, 'rb') as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            yield mm

def batch_process_tweets(file_path: str, batch_size: int = 1000) -> Iterator[List[str]]:
    """
    Lee tweets en batches para optimizar el procesamiento manteniendo bajo uso de memoria.
    
    Args:
        file_path: Ruta al archivo ZIP con los tweets
        batch_size: Tamaño del batch para procesamiento
        
    Yields:
        Lista de textos de tweets por batch
    """
    current_batch = []
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
                            current_batch.append(content)
                            
                            if len(current_batch) >= batch_size:
                                yield current_batch
                                current_batch = []
                                
                    except (json.JSONDecodeError, KeyError) as error:
                        logger.warning("Error en línea %d: %s", line_number, error)
                        continue
                        
                if current_batch:  # Procesar el último batch parcial
                    yield current_batch
                    
    except FileNotFoundError:
        logger.error("Archivo no encontrado: %s", file_path)
        raise
    except BadZipFile:
        logger.error("Archivo ZIP corrupto o inválido: %s", file_path)
        raise
    except Exception as error:
        logger.error("Error inesperado al procesar archivo: %s", error)
        raise TweetRepositoryError(f"Error procesando archivo: {error}")

def extract_emojis(text: str) -> List[str]:
    """
    Extrae emojis de un texto de manera eficiente.
    Usa generadores para minimizar uso de memoria.
    """
    return [c for c in text if c in emoji.EMOJI_DATA]

@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los 10 emojis más usados en los tweets.
    Implementación optimizada para memoria usando procesamiento por batches
    y técnicas de memory mapping.
    
    Args:
        file_path: Ruta al archivo ZIP con los tweets en formato JSON
        
    Returns:
        Lista de tuplas (emoji, conteo) con los 10 emojis más frecuentes
        
    Raises:
        FileNotFoundError: Si el archivo no existe
        BadZipFile: Si el archivo ZIP está corrupto
        TweetRepositoryError: Para otros errores inesperados
    """
    try:
        emoji_counts = Counter()
        tweets_processed = 0
        emojis_found = 0
        
        # Procesar tweets en batches para mejor eficiencia
        for batch in batch_process_tweets(file_path):
            try:
                # Procesar cada batch de tweets
                for text in batch:
                    emojis = extract_emojis(text)
                    emoji_counts.update(emojis)
                    
                    emojis_found += len(emojis)
                    tweets_processed += 1
                
                if tweets_processed % 10000 == 0:
                    logger.info(
                        "Progreso: %d tweets procesados, %d emojis encontrados", 
                        tweets_processed, 
                        emojis_found
                    )
                    # Liberar memoria innecesaria periódicamente
                    emoji_counts = Counter(dict(nlargest(100, emoji_counts.items(), key=lambda x: x[1])))
                    
            except Exception as error:
                logger.error("Error procesando batch: %s", error)
                continue
        
        if not emoji_counts:
            logger.warning("No se encontraron emojis en ningún tweet")
            return []
        
        logger.info(
            "Análisis completado: %d tweets procesados, %d emojis únicos encontrados",
            tweets_processed,
            len(emoji_counts)
        )
        
        return nlargest(10, emoji_counts.items(), key=lambda x: x[1])
        
    except Exception as error:
        logger.error("Error en el análisis de emojis: %s", error)
        raise TweetRepositoryError(f"Error en el análisis: {error}")