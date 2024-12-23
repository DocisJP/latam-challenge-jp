from typing import List, Tuple
from datetime import datetime
import pandas as pd
from memory_profiler import profile

@profile
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Encuentra las 10 fechas con más tweets y el usuario con más publicaciones por fecha.
    Implementación optimizada para tiempo usando vectorización.
    """
    # Leer directamente a DataFrame sin pasar por objetos Pydantic
    df = pd.read_json(file_path, lines=True)
    
    # Convertir date a fecha y extraer username en una sola operación
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['username'] = df['user'].apply(lambda x: x.get('username', ''))
    
    # Agregar contador por fecha y usuario en una operación
    grouped = df.groupby(['date', 'username']).size().reset_index(name='count')
    
    # Encontrar las top 10 fechas
    top_dates = grouped.groupby('date')['count'].sum().nlargest(10).index
    
    # Filtrar solo las fechas top y encontrar usuario top por fecha
    filtered = grouped[grouped['date'].isin(top_dates)]
    result = (filtered.sort_values(['date', 'count'], ascending=[True, False])
              .groupby('date').first()
              .reset_index()[['date', 'username']])
    
    return list(zip(result['date'], result['username']))