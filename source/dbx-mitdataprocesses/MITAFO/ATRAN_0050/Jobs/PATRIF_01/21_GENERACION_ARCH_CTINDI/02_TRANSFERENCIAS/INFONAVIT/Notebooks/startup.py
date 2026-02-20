# Databricks notebook source
import os
import sys
import importlib

def find_and_add_to_syspath(base_dir="MITAFO", target_dir="CGRLS_0010"):
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
                    root, target_dir
                )  # Obtener la ruta completa a CGRLS_0010
                sys.path.append(target_path)  # Agregar a sys.path
                return  # Termina la búsqueda tras encontrarlo

# Ejecutar la función para asegurarnos de que el path correcto está en `sys.path`
find_and_add_to_syspath()

# 🔹 Importar módulos
from modules import (
    logger,
    DBXConnectionManager,
    SETTINGS,
    custom_exception_handler,
    WidgetParams,
    Notify,
    QueryManager,
    ConfManager,
    CleanUpManager,  # 🚀 Importamos la clase de limpieza
    FileManager,
)

# 🔹 Configurar el manejador de excepciones global
sys.excepthook = custom_exception_handler
