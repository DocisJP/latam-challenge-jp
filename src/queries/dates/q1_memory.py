from typing import List, Tuple, Dict, Generator
from datetime import datetime
from collections import defaultdict
from memory_profiler import profile
import json
from zipfile import ZipFile

def read_tweets(file_path: str) -> Generator[tuple, None, None]:
    """Lee tweets uno a uno usando un generador."""
    with ZipFile(file_path) as zip_file:
        json_filename = zip_file.namelist()[0]
        with zip_file.open(json_filename) as json_file:
            for line in json_file:
                try:
                    tweet = json.loads(line.decode('utf-8'))
                    yield (
                        datetime.fromisoformat(tweet['date'].replace('Z', '+00:00')).date(),
                        tweet['user'].get('username', '')
                    )
                except Exception:
                    continue

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Encuentra las 10 fechas con más tweets y el usuario con más publicaciones por fecha.
    Implementación optimizada para memoria usando streaming y procesamiento por lotes.
    """
    # Diccionarios para mantener solo la información necesaria
    date_counts: Dict[datetime.date, int] = defaultdict(int)
    date_users: Dict[datetime.date, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    
    # Procesar tweets en streaming
    for tweet_date, username in read_tweets(file_path):
        date_counts[tweet_date] += 1
        date_users[tweet_date][username] += 1
        
        # Mantener solo las top 20 fechas para ahorrar memoria
        if len(date_counts) > 20:
            min_count = min(date_counts.values())
            dates_to_remove = [
                d for d, c in date_counts.items() 
                if c == min_count
            ][:len(date_counts) - 20]
            
            for date in dates_to_remove:
                del date_counts[date]
                del date_users[date]
    
    # Obtener top 10 fechas
    top_dates = sorted(
        date_counts.items(),
        key=lambda x: (-x[1], x[0])  # Ordenar por conteo desc, fecha asc
    )[:10]
    
    # Obtener usuario top por fecha
    return [
        (date, max(
            date_users[date].items(),
            key=lambda x: (x[1], x[0])  # Ordenar por conteo desc, username asc
        )[0])
        for date, _ in top_dates
    ] 