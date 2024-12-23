class TweetRepositoryError(Exception):
    """Excepción base para errores relacionados con el repositorio de tweets."""
    pass

class FileNotFoundError(TweetRepositoryError):
    """El archivo de tweets no fue encontrado."""
    pass

class InvalidFileFormatError(TweetRepositoryError):
    """El formato del archivo no es válido."""
    pass

class JsonParsingError(TweetRepositoryError):
    """Error al parsear el JSON."""
    pass 