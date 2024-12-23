from typing import List, Tuple
import pandas as pd
import emoji
import logging
import multiprocessing
from memory_profiler import profile
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from src.utils.exceptions import TweetRepositoryError
import numpy as np

logger = logging.getLogger(__name__)

def process_chunk(texts: List[str]) -> Counter:
    """
    Procesa un chunk de textos para extraer y contar emojis.
    
    Args:
        texts: Lista de textos a procesar
        
    Returns:
        Counter con el conteo de emojis en el chunk
    """
    chunk_counter = Counter()
    for text in texts:
        if not isinstance(text, str):
            continue
        emojis = [c for c in text if c in emoji.EMOJI_DATA]
        chunk_counter.update(emojis)
    return chunk_counter

@profile
def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los 10 emojis más usados usando procesamiento paralelo.
    
    Args:
        file_path: Ruta al archivo ZIP con los tweets
        
    Returns:
        Lista de tuplas (emoji, conteo) con los 10 emojis más frecuentes
        
    Raises:
        FileNotFoundError: Si el archivo no existe
        TweetRepositoryError: Para otros errores de procesamiento
    """
    try:
        logger.info("Iniciando lectura del archivo de tweets")
        df = pd.read_json(file_path, lines=True)
        logger.info("Leídos %s tweets", len(df))
        
        try:
            if 'content' not in df.columns:
                raise TweetRepositoryError("El archivo no contiene la columna 'content'")
            
            logger.info("Iniciando procesamiento paralelo de emojis")
            
            # Determinar número óptimo de cores
            n_cores = max(multiprocessing.cpu_count() - 1, 1)
            logger.info(f"Utilizando {n_cores} cores para procesamiento paralelo")
            
            # Calcular tamaño óptimo de chunks basado en la cantidad de datos
            total_rows = len(df)
            chunk_size = max(total_rows // (n_cores * 2), 1000)  # Mínimo 1000 registros por chunk
            n_chunks = (total_rows + chunk_size - 1) // chunk_size  # Redondeo hacia arriba
            content_chunks = np.array_split(df['content'].values, n_chunks)
            
            # Counter total para combinar resultados
            total_counter = Counter()
            tweets_procesados = 0
            
            with ProcessPoolExecutor(max_workers=n_cores) as executor:
                # Crear futures para cada chunk
                future_to_chunk = {
                    executor.submit(process_chunk, chunk.tolist()): i 
                    for i, chunk in enumerate(content_chunks)
                }
                
                # Procesar resultados a medida que completan
                for future in as_completed(future_to_chunk):
                    chunk_idx = future_to_chunk[future]
                    try:
                        chunk_counter = future.result()
                        total_counter.update(chunk_counter)
                        tweets_procesados += len(content_chunks[chunk_idx])
                        logger.info("Procesado chunk %d, total tweets: %d", 
                                  chunk_idx, tweets_procesados)
                    except Exception as e:
                        logger.error("Error en chunk %d: %s", chunk_idx, e)
            
            if not total_counter:
                logger.warning("No se encontraron emojis en ningún tweet")
                return []
            
            logger.info("Análisis completado: %s tweets procesados", tweets_procesados)
            return total_counter.most_common(10)
            
        except AttributeError as error:
            logger.error("Error en el formato de los datos: %s", error)
            raise TweetRepositoryError(f"Formato de datos inválido: {error}")
        except Exception as error:
            logger.error("Error procesando los tweets: %s", error)
            raise TweetRepositoryError(f"Error en el procesamiento: {error}")
            
    except pd.errors.EmptyDataError:
        logger.error("El archivo está vacío")
        return []
    except pd.errors.ParserError as error:
        logger.error("Error parseando el archivo JSON: %s", error)
        raise TweetRepositoryError(f"Error en el formato del archivo: {error}")
    except FileNotFoundError:
        logger.error("Archivo no encontrado: %s", file_path)
        raise
    except Exception as error:
        logger.error("Error inesperado: %s", error)
        raise TweetRepositoryError(f"Error inesperado en el análisis: {error}")