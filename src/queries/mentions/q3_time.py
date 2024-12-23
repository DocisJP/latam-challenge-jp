from typing import List, Tuple
import pandas as pd
import numpy as np
import logging
from memory_profiler import profile
import re
import multiprocessing
from src.utils.exceptions import TweetRepositoryError

logger = logging.getLogger(__name__)

def extract_mentions_fast(text: str, pattern: re.Pattern) -> np.ndarray:
    """Extracción rápida de menciones usando regex precompilado."""
    return pattern.findall(text) if isinstance(text, str) else []

def process_chunk_parallel(args):
    """Procesamiento paralelo de chunks con vectorización."""
    chunk, pattern = args
    mentions = []
    for text in chunk:
        mentions.extend(extract_mentions_fast(text, pattern))
    return pd.Series(mentions).value_counts()

@profile
def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los 10 usuarios más mencionados usando procesamiento híbrido.
    Optimizado para máxima velocidad usando memoria preallocation y procesamiento paralelo.
    
    Args:
        file_path: Ruta al archivo ZIP con los tweets
        
    Returns:
        Lista de tuplas (username, conteo_menciones) con los 10 usuarios más mencionados
    """
    try:
        logger.info("Iniciando lectura del archivo de tweets")
        
        # 1. Lectura completa en memoria
        df = pd.read_json(file_path, lines=True)
        logger.info(f"Leídos {len(df)} tweets")
        
        # 2. Precompilación de regex y preparación de chunks
        pattern = re.compile(r'@([a-zA-Z0-9_]+)')
        n_cores = multiprocessing.cpu_count()
        chunk_size = max(1000, len(df) // (n_cores * 4))
        chunks = np.array_split(df['content'].values, n_cores * 4)
        
        # 3. Procesamiento paralelo con preallocación
        logger.info(f"Iniciando procesamiento paralelo con {n_cores} cores")
        with multiprocessing.Pool(n_cores) as pool:
            # Usar imap_unordered para mejor rendimiento
            chunk_results = list(pool.imap_unordered(
                process_chunk_parallel,
                [(chunk, pattern) for chunk in chunks],
                chunksize=1
            ))
        
        # 4. Combinar resultados eficientemente
        logger.info("Combinando resultados...")
        final_counts = pd.concat(chunk_results).groupby(level=0).sum()
        
        # 5. Ordenar y obtener top 10
        top_mentions = final_counts.nlargest(10)
        
        logger.info(f"Análisis completado: encontradas {len(final_counts)} menciones únicas")
        return list(zip(top_mentions.index, top_mentions.values))
        
    except Exception as error:
        logger.error(f"Error en el análisis: {error}")
        raise TweetRepositoryError(f"Error en el análisis: {error}")