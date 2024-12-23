from abc import ABC, abstractmethod
from typing import List, Tuple
from datetime import datetime

from src.queries.dates import q1_time, q1_memory
from src.queries.emojis import q2_time, q2_memory
from src.queries.mentions import q3_time, q3_memory

class TweetAnalyzer(ABC):
    """Clase base abstracta que define la interfaz para operaciones de análisis de tweets."""
    
    @abstractmethod
    def q1_time(self, file_path: str) -> List[Tuple[datetime.date, str]]:
        """Encuentra las 10 fechas con más tweets y el usuario con más publicaciones por fecha.
        
        Args:
            file_path: Ruta al archivo JSON que contiene los tweets
            
        Returns:
            Lista de tuplas con (fecha, nombre_usuario) ordenada por cantidad de tweets
        """
        pass

    @abstractmethod
    def q1_memory(self, file_path: str) -> List[Tuple[datetime.date, str]]:
        """Versión optimizada en memoria de q1_time."""
        pass

    @abstractmethod
    def q2_time(self, file_path: str) -> List[Tuple[str, int]]:
        """Encuentra los 10 emojis más usados con su conteo.
        
        Args:
            file_path: Ruta al archivo JSON que contiene los tweets
            
        Returns:
            Lista de tuplas con (emoji, conteo) ordenada por conteo
        """
        pass

    @abstractmethod
    def q2_memory(self, file_path: str) -> List[Tuple[str, int]]:
        """Versión optimizada en memoria de q2_time."""
        pass

    @abstractmethod
    def q3_time(self, file_path: str) -> List[Tuple[str, int]]:
        """Encuentra los 10 usuarios más mencionados por cantidad de menciones.
        
        Args:
            file_path: Ruta al archivo JSON que contiene los tweets
            
        Returns:
            Lista de tuplas con (nombre_usuario, conteo_menciones) ordenada por conteo
        """
        pass

    @abstractmethod
    def q3_memory(self, file_path: str) -> List[Tuple[str, int]]:
        """Versión optimizada en memoria de q3_time."""
        pass

class TweetAnalyzerImpl(TweetAnalyzer):
    """Implementación concreta del analizador de tweets."""
    
    def q1_time(self, file_path: str) -> List[Tuple[datetime.date, str]]:
        return q1_time(file_path)
        
    def q1_memory(self, file_path: str) -> List[Tuple[datetime.date, str]]:
        return q1_memory(file_path)
        
    def q2_time(self, file_path: str) -> List[Tuple[str, int]]:
        return q2_time(file_path)
        
    def q2_memory(self, file_path: str) -> List[Tuple[str, int]]:
        return q2_memory(file_path)
        
    def q3_time(self, file_path: str) -> List[Tuple[str, int]]:
        return q3_time(file_path)
        
    def q3_memory(self, file_path: str) -> List[Tuple[str, int]]:
        return q3_memory(file_path)