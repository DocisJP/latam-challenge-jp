import time
import argparse
from typing import Any, Callable, Dict, List, Tuple
from datetime import date

from src.queries.dates import q1_time, q1_memory
# from src.queries.emojis import q2_time, q2_memory
# from src.queries.mentions import q3_time, q3_memory

QueryFunction = Callable[[str], List[Tuple[Any, Any]]]

AVAILABLE_QUERIES: Dict[str, Dict[str, QueryFunction]] = {
    'q1': {
        'time': q1_time,
        'memory': q1_memory
    },
    # 'q2': {
    #     'time': q2_time,
    #     'memory': q2_memory
    # },
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
    """
    start_time = time.time()
    result = func(file_path)
    execution_time = time.time() - start_time
    return result, execution_time

def display_results(results: List[Tuple[Any, Any]], execution_time: float) -> None:
    """
    Muestra los resultados formateados.
    
    Args:
        results: Lista de resultados
        execution_time: Tiempo de ejecución
    """
    print(f"\nTiempo de ejecución: {execution_time:.2f} segundos")
    print("\nResultados:")
    print("-" * 50)
    for key, value in results:
        print(f"{key}: {value}")

def execute_query(query: str, optimization: str, file_path: str) -> None:
    """
    Ejecuta una consulta específica con la optimización indicada.
    
    Args:
        query: Identificador de la consulta (q1, q2, q3)
        optimization: Tipo de optimización (time, memory)
        file_path: Ruta al archivo de datos
    """
    if query not in AVAILABLE_QUERIES:
        raise ValueError(f"Query {query} no disponible. Opciones: {list(AVAILABLE_QUERIES.keys())}")
    
    if optimization not in AVAILABLE_QUERIES[query]:
        raise ValueError(f"Optimización {optimization} no disponible para {query}")
    
    print(f"\n=== Ejecutando {query} (Optimizado para {optimization}) ===")
    query_func = AVAILABLE_QUERIES[query][optimization]
    results, exec_time = measure_execution_time(query_func, file_path)
    display_results(results, exec_time)

def main() -> None:
    """Punto de entrada principal del programa."""
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

if __name__ == "__main__":
    main() 