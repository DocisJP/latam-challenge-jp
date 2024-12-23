from typing import List, Tuple
from datetime import datetime
import logging
import pandas as pd
from memory_profiler import profile
from src.utils.exceptions import TweetRepositoryError

logger = logging.getLogger(__name__)

@profile
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Encuentra las 10 fechas con más tweets y el usuario con más publicaciones por fecha.
    Implementación optimizada para tiempo usando vectorización.
    
    Args:
        file_path: Ruta al archivo ZIP con los tweets en formato JSON
        
    Returns:
        Lista de tuplas (fecha, username) con las 10 fechas con más tweets
        
    Raises:
        FileNotFoundError: Si el archivo no existe
        TweetRepositoryError: Para otros errores de procesamiento
    """
    try:
        logger.info("Iniciando lectura del archivo de tweets")
        # Leer directamente a DataFrame sin pasar por objetos Pydantic
        df = pd.read_json(file_path, lines=True)
        logger.info("Leídos %s tweets", len(df))
        
        try:
            # Validar columnas requeridas
            required_columns = ['date', 'user']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise TweetRepositoryError(f"Columnas faltantes: {', '.join(missing_columns)}")
            
            logger.info("Procesando fechas y usuarios")
            try:
                # Convertir date a fecha y extraer username en una sola operación
                df['date'] = pd.to_datetime(df['date']).dt.date
                df['username'] = df['user'].apply(lambda x: x.get('username', ''))
                
                # Agregar contador por fecha y usuario en una operación
                logger.info("Calculando conteos por fecha y usuario")
                grouped = df.groupby(['date', 'username']).size().reset_index(name='count')
                
                # Encontrar las top 10 fechas
                logger.info("Identificando las 10 fechas con más tweets")
                date_counts = grouped.groupby('date')['count'].sum()
                top_dates = date_counts.nlargest(10)
                
                if top_dates.empty:
                    logger.warning("No se encontraron fechas con tweets")
                    return []
                    
                logger.info("Encontradas %s fechas top", len(top_dates))
                
                # Filtrar solo las fechas top y encontrar usuario top por fecha
                filtered = grouped[grouped['date'].isin(top_dates.index)]
                result = (filtered.sort_values(['date', 'count'], ascending=[True, False])
                         .groupby('date').first()
                         .reset_index()[['date', 'username']])
                
                logger.info("Análisis completado exitosamente")
                return list(zip(result['date'], result['username']))
                
            except (AttributeError, KeyError) as error:
                logger.error("Error procesando datos: %s", error)
                raise TweetRepositoryError(f"Error en el procesamiento de datos: {error}")
                
        except pd.errors.EmptyDataError:
            logger.error("No se encontraron datos válidos")
            return []
        except Exception as error:
            logger.error("Error en el análisis de datos: %s", error)
            raise TweetRepositoryError(f"Error en el análisis: {error}")
            
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