import sys
import logging
import os
from py4j.protocol import Py4JNetworkError
from io import StringIO

# Eliminar handlers existentes para evitar conflictos previos
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configuración del logger global
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Crear un logger centralizado
logger = logging.getLogger("project_logger")

# Reducir el nivel de logging de Py4J
py4j_logger = logging.getLogger("py4j")
py4j_logger.setLevel(logging.ERROR)

# Filtrar mensajes ruidosos de Py4J
class Py4JNoiseFilter(logging.Filter):
    def filter(self, record):
        excluded_messages = [
            "Connection reset by peer",
            "Py4JNetworkError",
            "Error while sending or receiving",
            "Closing down clientserver connection",
            "Exception while sending command",
        ]
        return not any(msg in record.getMessage() for msg in excluded_messages)

# Aplicar el filtro a todos los loggers
logger.addFilter(Py4JNoiseFilter())
py4j_logger.addFilter(Py4JNoiseFilter())
logging.root.addFilter(Py4JNoiseFilter())

# Redirigir stderr para filtrar mensajes ruidosos
class StderrInterceptor:
    def __init__(self):
        self.original_stderr = sys.stderr
        self.buffer = StringIO()

    def write(self, message):
        # Filtrar mensajes específicos
        excluded_messages = [
            "Connection reset by peer",
            "Py4JNetworkError",
            "Error while sending or receiving",
            "Closing down clientserver connection",
            "Exception while sending command",
        ]
        if not any(msg in message for msg in excluded_messages):
            self.original_stderr.write(message)

    def flush(self):
        self.original_stderr.flush()

# Interceptar stderr
sys.stderr = StderrInterceptor()