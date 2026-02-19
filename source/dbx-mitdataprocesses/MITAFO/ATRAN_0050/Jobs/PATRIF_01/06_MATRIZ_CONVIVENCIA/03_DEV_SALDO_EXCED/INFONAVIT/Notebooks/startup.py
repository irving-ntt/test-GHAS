# Databricks notebook source
import os
import sys
import importlib
import inspect
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import length, lit, current_timestamp, concat, col, when, lead, monotonically_increasing_id, from_utc_timestamp, to_timestamp, row_number
from pyspark.sql.window import Window
from pyspark import StorageLevel

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


# Ejecutar la función
find_and_add_to_syspath()
from modules import (
    logger,
    DBXConnectionManager,
    SETTINGS,
    custom_exception_handler,
    WidgetParams,
    Notify,
    QueryManager,
    ConfManager,
    FileManager,
)

sys.excepthook = custom_exception_handler

# COMMAND ----------


