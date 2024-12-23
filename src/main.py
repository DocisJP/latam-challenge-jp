import time
import argparse
import logging
import sys
from typing import Any, Callable, Dict, List, Tuple
from zipfile import BadZipFile

from src.queries.dates import q1_time, q1_memory
from src.queries.emojis import q2_time, q2_memory
from src.utils.exceptions import TweetRepositoryError
# from src.queries.mentions import q3_time, q3_memory

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

QueryFunction = Callable[[str], List[Tuple[Any, Any]]]

AVAILABLE_QUERIES: Dict[str, Dict[str, QueryFunction]] = {
    'q1': {
        'time': q1_time,
        'memory': q1_memory
    },
    'q2': {
         'time': q2_time,
         'memory': q2_memory
    },
    # 'q3': {
    #     'time': q3_time,
    #     'memory': q3_memory
    # }
}

def measure_execution_time(func: QueryFunction, file_path: str) -> Tuple[List[Tuple[Any, Any]], float]:
    """
    Mide el tiempo de ejecución de una función.
    
    Args:
        func: Función a medir
        file_path: Ruta al archivo de datos
        
    Returns:
        Tupla con los resultados y el tiempo de ejecución
        
    Raises:
        TweetRepositoryError: Si hay errores durante el procesamiento
    """
    try:
        logger.info(f"Iniciando ejecución de {func.__name__}")
        start_time = time.time()
        result = func(file_path)
        execution_time = time.time() - start_time
        logger.info(f"Ejecución completada en {execution_time:.2f} segundos")
        return result, execution_time
    except Exception as error:
        logger.error(f"Error durante la ejecución de {func.__name__}: {error}")
        raise

def display_results(results: List[Tuple[Any, Any]], execution_time: float, query: str) -> None:
    """
    Muestra los resultados formateados.
    
    Args:
        results: Lista de resultados
        execution_time: Tiempo de ejecución
        query: Tipo de consulta ejecutada
    """
    try:
        print(f"\nTiempo de ejecución: {execution_time:.2f} segundos")
        print("\nResultados:")
        print("-" * 50)
        
        if not results:
            print("No se encontraron resultados.")
            return
        
        if query == 'q2':
            print("Top 10 emojis más usados:")
            for emoji, count in results:
                print(f"{emoji}: {count} veces")
        else:
            for key, value in results:
                print(f"{key}: {value}")
    except Exception as error:
        logger.error(f"Error mostrando resultados: {error}")
        print("Error al mostrar los resultados. Consulte los logs para más detalles.")

def execute_query(query: str, optimization: str, file_path: str) -> None:
    """
    Ejecuta una consulta específica con la optimización indicada.
    
    Args:
        query: Identificador de la consulta (q1, q2, q3)
        optimization: Tipo de optimización (time, memory)
        file_path: Ruta al archivo de datos
        
    Raises:
        ValueError: Si la query u optimización no están disponibles
        FileNotFoundError: Si el archivo no existe
        TweetRepositoryError: Para otros errores de procesamiento
    """
    if query not in AVAILABLE_QUERIES:
        raise ValueError(f"Query {query} no disponible. Opciones: {list(AVAILABLE_QUERIES.keys())}")
    
    if optimization not in AVAILABLE_QUERIES[query]:
        raise ValueError(f"Optimización {optimization} no disponible para {query}")
    
    logger.info(f"Ejecutando {query} (Optimizado para {optimization})")
    query_func = AVAILABLE_QUERIES[query][optimization]
    
    try:
        results, exec_time = measure_execution_time(query_func, file_path)
        display_results(results, exec_time, query)
    except FileNotFoundError:
        logger.error(f"No se encontró el archivo: {file_path}")
        print(f"\nError: No se encontró el archivo {file_path}")
        sys.exit(1)
    except BadZipFile:
        logger.error(f"El archivo está corrupto o no es un ZIP válido: {file_path}")
        print(f"\nError: El archivo {file_path} está corrupto o no es un ZIP válido")
        sys.exit(1)
    except TweetRepositoryError as error:
        logger.error(f"Error procesando tweets: {error}")
        print(f"\nError: {error}")
        sys.exit(1)
    except Exception as error:
        logger.error(f"Error inesperado: {error}")
        print("\nError inesperado. Consulte los logs para más detalles.")
        sys.exit(1)

def main() -> None:
    """Punto de entrada principal del programa."""
    try:
        parser = argparse.ArgumentParser(description='Análisis de tweets')
        parser.add_argument(
            '--query',
            choices=list(AVAILABLE_QUERIES.keys()),
            required=True,
            help='Query a ejecutar (q1, q2, q3)'
        )
        parser.add_argument(
            '--optimization',
            choices=['time', 'memory'],
            required=True,
            help='Tipo de optimización a usar'
        )
        parser.add_argument(
            '--file',
            default="data/tweets.json.zip",
            help='Ruta al archivo de tweets'
        )
        
        args = parser.parse_args()
        execute_query(args.query, args.optimization, args.file)
    except KeyboardInterrupt:
        logger.info("Ejecución interrumpida por el usuario")
        print("\nEjecución interrumpida por el usuario")
        sys.exit(0)
    except Exception as error:
        logger.error(f"Error en la ejecución principal: {error}")
        print(f"\nError: {error}")
        sys.exit(1)

if __name__ == "__main__":
    main() 