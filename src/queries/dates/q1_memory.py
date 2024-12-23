from typing import List, Tuple, Iterator
from datetime import datetime
from collections import Counter
from memory_profiler import profile
import json
from zipfile import ZipFile
from heapq import nlargest

def read_tweets(file_path: str) -> Iterator[tuple]:
    """Lee tweets de manera eficiente usando iterador."""
    with ZipFile(file_path) as zip_file:
        json_filename = zip_file.namelist()[0]
        with zip_file.open(json_filename) as json_file:
            for line in json_file:
                try:
                    tweet = json.loads(line)
                    yield (
                        datetime.fromisoformat(tweet['date'].replace('Z', '+00:00')).date(),
                        tweet['user']['username']
                    )
                except (KeyError, json.JSONDecodeError):
                    continue

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Encuentra las 10 fechas con más tweets y el usuario con más publicaciones por fecha.
    Implementación optimizada para memoria usando streaming y contadores eficientes.
    """
    # Usar Counter en lugar de defaultdict
    date_counts = Counter()
    current_date = None
    current_users = Counter()
    date_top_users = {}
    
    # Procesar tweets en streaming, manteniendo solo un día en memoria
    for tweet_date, username in read_tweets(file_path):
        # Contar fecha
        date_counts[tweet_date] += 1
        
        # Si cambiamos de fecha, procesamos los usuarios del día anterior
        if current_date != tweet_date:
            if current_date and current_users:
                # Guardar solo el usuario top para la fecha
                date_top_users[current_date] = current_users.most_common(1)[0][0]
                current_users.clear()
            current_date = tweet_date
        
        current_users[username] += 1

    # Procesar el último día
    if current_users:
        date_top_users[current_date] = current_users.most_common(1)[0][0]
    
    # Obtener top 10 fechas usando nlargest
    top_dates = nlargest(10, date_counts.items(), key=lambda x: (x[1], x[0]))
    
    # Construir resultado final
    return [(date, date_top_users[date]) for date, _ in top_dates]