import os
import re

from logger import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from databricks.sdk.runtime import dbutils


class QueryManager:
    """
    Clase para manejar consultas SQL dentro de la carpeta 'SQL' y procesar sus parámetros.
    """

    def __init__(self):
        """
        Constructor de QueryManager. Carga automáticamente la lista de archivos SQL
        y sus parámetros en el objeto.
        """
        self.sql_path_dbfs = self._get_sql_folder_path()
        self.queries = self._get_sql_list()

    def _get_sql_folder_path(self):
        """
        Obtiene la ruta de la carpeta SQL.
        """
        try:
            notebook_path = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
        except Exception as e:
            raise RuntimeError(
                f"No se pudo obtener el path del notebook. Error: {str(e)}"
            )

        path_parts = notebook_path.split("/")
        if "Notebooks" not in path_parts:
            raise FileNotFoundError(
                f"La carpeta 'Notebooks' no se encontró en el path: {notebook_path}"
            )

        notebooks_index = path_parts.index("Notebooks")
        base_dir = "/".join(path_parts[:notebooks_index])  # Directorio raíz

        return f"/Workspace{base_dir}/SQL"

    def _get_sql_list(self):
        """
        Obtiene la lista de archivos SQL en la carpeta 'SQL' y extrae los parámetros.

        :return: DataFrame de PySpark con:
                 - "Archivo SQL": Nombre del archivo SQL.
                 - "Parámetros": Lista de parámetros detectados dentro de ##.
        """
        spark = SparkSession.getActiveSession()

        if not os.path.exists(self.sql_path_dbfs):
            logger.warning(f"La carpeta 'SQL' no existe en {self.sql_path_dbfs}")
            empty_schema = StructType(
                [
                    StructField("Archivo SQL", StringType(), True),
                    StructField("Parámetros", ArrayType(StringType()), True),
                ]
            )
            return spark.createDataFrame([], schema=empty_schema)

        sql_files = [f for f in os.listdir(self.sql_path_dbfs) if f.endswith(".sql")]

        if not sql_files:
            logger.warning(f"No se encontraron archivos .sql en {self.sql_path_dbfs}")
            empty_schema = StructType(
                [
                    StructField("Archivo SQL", StringType(), True),
                    StructField("Parámetros", ArrayType(StringType()), True),
                ]
            )
            return spark.createDataFrame([], schema=empty_schema)

        # Crear la lista de tuplas (archivo, parámetros extraídos)
        files_with_params = [
            (file, self._extract_parameters(os.path.join(self.sql_path_dbfs, file)))
            for file in sql_files
        ]

        # Definir el esquema explícitamente para evitar errores de inferencia de tipos
        schema = StructType(
            [
                StructField("Archivo SQL", StringType(), True),
                StructField("Parámetros", ArrayType(StringType()), True),
            ]
        )

        return spark.createDataFrame(files_with_params, schema)

    @staticmethod
    def _extract_parameters(file_path):
        """
        Método interno para leer un archivo SQL y extraer los parámetros dentro de ##.

        :param file_path: Ruta completa del archivo SQL.
        :return: Lista de parámetros encontrados en el archivo.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                params = re.findall(
                    r"#(.*?)#", content
                )  # Buscar parámetros dentro de ##
                return params
        except Exception as e:
            logger.warning(f"No se pudo leer {file_path}. Error: {e}")
            return []

    def get_sql_list(self):
        """
        Devuelve la lista de archivos SQL y sus parámetros detectados.

        :return: DataFrame de PySpark con:
                 - "Archivo SQL"
                 - "Parámetros"
        """
        return self.queries

    def get_statement(self, sql_filename, hints=None, **params):
        """
        Carga un archivo SQL y reemplaza los parámetros dentro de ## con los valores proporcionados.
        También permite agregar hints SQL opcionales para SELECT, DELETE, INSERT y UPDATE.

        :param sql_filename: Nombre del archivo SQL.
        :param hints: Hints SQL opcionales.
        :param params: Diccionario de valores para reemplazar en el query.
        :return: Query con los parámetros reemplazados.
        """
        sql_path = os.path.join(self.sql_path_dbfs, sql_filename)

        if not os.path.exists(sql_path):
            raise FileNotFoundError(
                f"El archivo {sql_filename} no existe en la carpeta SQL."
            )

        try:
            with open(sql_path, "r", encoding="utf-8") as f:
                query = f.read()

                # Reemplazar parámetros dentro de ## con los valores proporcionados
                for key, value in params.items():
                    query = query.replace(f"#{key}#", str(value))

                # Insertar hint según el tipo de query
                if hints:
                    query = self._insert_hints(query, hints)

                print("\n📜 **Query generado:**\n")
                print(query)  # Se imprimirá automáticamente el query formateado

                return query
        except Exception as e:
            raise RuntimeError(f"No se pudo leer o procesar {sql_filename}. Error: {e}")

    @staticmethod
    def _insert_hints(query, hints):
        """
        Inserta hints en la posición correcta según el tipo de query.

        :param query: Query original.
        :param hints: Hints SQL opcionales.
        :return: Query modificado con los hints aplicados correctamente.
        """
        # Para SELECT - buscar en cualquier línea, no solo al inicio
        if re.search(r"(?i)\bSELECT\b", query):
            return re.sub(r"(?i)\bSELECT\b", f"SELECT {hints}", query, count=1)

        # Para DELETE → Insertar hint justo después de DELETE
        if re.search(r"(?i)\bDELETE\s+FROM\b", query):
            return re.sub(r"(?i)\b(DELETE)", rf"\1 {hints}", query, count=1)

        # Para UPDATE
        if re.search(r"(?i)\bUPDATE\b", query):
            return re.sub(r"(?i)\b(UPDATE\s+\w+)", rf"\1 {hints}", query, count=1)

        # Para INSERT
        if re.search(r"(?i)\bINSERT\s+INTO\b", query):
            return re.sub(
                r"(?i)\b(INSERT\s+INTO\s+\w+)", rf"\1 {hints}", query, count=1
            )

        return query  # Si no coincide con ninguna regla, se deja sin cambios
