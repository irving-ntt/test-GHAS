import os
import sys


def find_and_add_to_syspath(
    base_dir="MITAFO", target_dir="CGRLS_0010", sub_dir="modules"
):
    current_path = os.getcwd()

    # Dividir el path en partes
    path_parts = current_path.split("/")

    # Buscar el índice del directorio base (MITAFO)
    if base_dir in path_parts:
        base_index = path_parts.index(base_dir)  # Obtener la posición de MITAFO
        search_path = "/".join(
            path_parts[: base_index + 1]
        )  # Construir el path hasta MITAFO

        # Recorrer los subdirectorios de MITAFO buscando CGRLS_0010
        for root, dirs, files in os.walk(search_path):
            if target_dir in dirs:
                target_path = os.path.join(
                    root, target_dir, sub_dir
                )  # Agregar "/modules" al final
                if os.path.exists(target_path):  # Verificar que el directorio existe
                    sys.path.append(target_path)  # Agregar a sys.path
                    return  # Termina la búsqueda tras encontrarlo


# Ejecutar la función
find_and_add_to_syspath()


# Importar módulos principales
from .logger import logger
from .dbxmanager import DBXConnectionManager
from .settings import SETTINGS
from .handlers import custom_exception_handler, WidgetParams
from .handlers import Notify
from .sqlmanager import QueryManager
from .confmanager import ConfManager
from .cleanupmanager import CleanUpManager
from .filemanager import FileManager


# Definir los elementos exportados
__all__ = [
    "logger",
    "DBXConnectionManager",
    "SETTINGS",
    "custom_exception_handler",
    "Notify",
    "QueryManager",
    "ConfManager",
    "CleanUpManager",
    "FileManager",
]