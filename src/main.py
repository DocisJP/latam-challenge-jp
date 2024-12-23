import time
import argparse
from src.queries.dates import q1_time, q1_memory

def time_function(func, file_path: str):
    """Mide el tiempo de ejecución de una función."""
    start_time = time.time()
    result = func(file_path)
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time

def format_results(results, execution_time: float):
    """Formatea los resultados con métricas."""
    print(f"\nTiempo de ejecución: {execution_time:.2f} segundos")
    print("\nTop 10 fechas con más tweets:")
    print("-" * 50)
    for date, user in results:
        print(f"{date}: {user}")

def run_with_profiling(file_path: str):
    """Ejecuta las funciones con profiling de memoria y tiempo."""
    print("\n=== Análisis Optimizado para Tiempo ===")
    results, exec_time = time_function(q1_time, file_path)
    format_results(results, exec_time)
    
    print("\n=== Análisis Optimizado para Memoria ===")
    results, exec_time = time_function(q1_memory, file_path)
    format_results(results, exec_time)

def main():
    parser = argparse.ArgumentParser(description='Análisis de tweets')
    parser.add_argument('--profile', action='store_true', 
                       help='Ejecutar con profiling de memoria y tiempo')
    parser.add_argument('--file', default="data/tweets.json.zip",
                       help='Ruta al archivo de tweets')
    
    args = parser.parse_args()
    run_with_profiling(args.file)

if __name__ == "__main__":
    main() 