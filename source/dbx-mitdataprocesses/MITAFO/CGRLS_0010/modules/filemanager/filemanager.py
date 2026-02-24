import hashlib
import time
from datetime import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from databricks.sdk.runtime import dbutils
from settings import SETTINGS
from logger import logger
import random

class FileManager:
    def __init__(self, err_repo_path=None):
        """
        Clase para la gestión de generación de archivos CTINDI.
        
        Args:
            err_repo_path (str, opcional): Ruta del repositorio de errores.
        """
        self.external_location = SETTINGS.GENERAL.EXTERNAL_LOCATION
        self.err_repo_path = err_repo_path
        self.catalog = SETTINGS.GENERAL.CATALOG
        self.schema = SETTINGS.GENERAL.SCHEMA
        self.spark = SparkSession.builder.getOrCreate()
    
    def generar_archivo_ctindi(self, df_final, full_file_name, header, calcular_md5=False):
        """
        Genera un archivo CTINDI basado en los parámetros proporcionados y lo guarda en la ubicación especificada.
        
        Args:
            df_final (DataFrame): DataFrame que se guardará como archivo.
            full_file_name (str): Ruta completa del archivo de salida.
            header (bool): Indica si el archivo CSV debe incluir encabezados.
            calcular_md5 (bool, opcional): Indica si se debe calcular el MD5 del archivo generado. Por defecto, False.
        
        Raises:
            Exception: Si ocurre un error durante la generación del archivo.
        """
        try:
            start_time = time.time()
            rand_num = random.randint(10000, 100000)
            
            logger.info(f"📂 Iniciando generación de archivo: {full_file_name}")
            
            # Escribir el DataFrame en la ruta temporal
            temp_start = time.time()
            df_final.coalesce(1).write.mode("overwrite").format("csv").option("header", header).option("lineSep", "\r\n").save(full_file_name + "_" + str(rand_num))
            temp_end = time.time()
            logger.info(f"✅ Archivo temporal generado en {temp_end - temp_start:.2f} segundos.")

            # Identificar el archivo CSV generado
            list_of_files = dbutils.fs.ls(full_file_name + "_" + str(rand_num))
            csv_part_file = next((item.path for item in list_of_files if item.name.startswith("part")), None)
            
            if not csv_part_file:
                raise FileNotFoundError(f"No se encontró el archivo generado en {full_file_name}_{str(rand_num)}.")

            # Obtener tamaño del archivo antes de moverlo
            file_size = next((item.size for item in list_of_files if item.path == csv_part_file), 0)
            file_size_mb = file_size / (1024 * 1024)  # Convertir bytes a MB
            logger.info(f"📦 Tamaño del archivo generado: {file_size_mb:.2f} MB")
            
            # Mover el archivo generado al destino final
            move_start = time.time()
            dbutils.fs.mv(csv_part_file, full_file_name, True)
            dbutils.fs.rm(full_file_name + "_" + str(rand_num), True)
            move_end = time.time()

            logger.info(f"🚀 Archivo final movido en {move_end - move_start:.2f} segundos.")
            
            total_time = time.time() - start_time
            logger.info(f"✅ Proceso completado en {total_time:.2f} segundos.")
            
            # Calcular y almacenar el MD5 del archivo generado si está activado
            if calcular_md5:
                full_file_name_md5 = full_file_name + ".md5"
                self.generar_md5(full_file_name, full_file_name_md5)
            
        except Exception as e:
            logger.error(f"❌ Error en la generación del archivo: {e}")
            raise

    
    def generar_archivo_flexible(self, df, full_file_name, opciones=None, calcular_md5=False):
        """
        Genera un archivo con opciones personalizadas y lo guarda en la ubicación especificada.
        
        Args:
            df (DataFrame): DataFrame que se guardará como archivo.
            full_file_name (str): Ruta completa del archivo de salida.
            opciones (dict, opcional): Diccionario con opciones adicionales para el write.
            calcular_md5 (bool, opcional): Si se desea calcular el hash MD5 del archivo generado.
        
        Raises:
            Exception: Si ocurre un error durante la generación del archivo.
        """
        try:
            start_time = time.time()
            rand_num = random.randint(10000, 100000)
            ruta_temporal = f"{full_file_name}_{rand_num}"

            logger.info(f"📂 Iniciando generación de archivo flexible: {full_file_name}")

            # Construir el writer con opciones
            writer = df.coalesce(1).write.mode("overwrite").format("csv")
            if opciones:
                for k, v in opciones.items():
                    writer = writer.option(k, v)
            else:
                writer = writer.option("header", "true").option("lineSep", "\r\n")

            writer.save(ruta_temporal)
            logger.info("✅ Archivo temporal generado.")

            # Identificar archivo part
            archivos = dbutils.fs.ls(ruta_temporal)
            archivo_csv = next((f.path for f in archivos if f.name.startswith("part")), None)
            if not archivo_csv:
                raise FileNotFoundError(f"No se encontró archivo 'part' en {ruta_temporal}")

            # Tamaño del archivo
            tamano_mb = next((f.size for f in archivos if f.path == archivo_csv), 0) / (1024 * 1024)
            logger.info(f"📦 Tamaño del archivo generado: {tamano_mb:.2f} MB")

            # Mover archivo
            dbutils.fs.mv(archivo_csv, full_file_name, True)
            dbutils.fs.rm(ruta_temporal, True)
            logger.info(f"🚀 Archivo final movido correctamente a {full_file_name}")

            # Calcular MD5
            if calcular_md5:
                self.generar_md5(full_file_name, full_file_name + ".md5")

            logger.info(f"✅ Proceso completado en {time.time() - start_time:.2f} segundos.")

        except Exception as e:
            logger.error(f"❌ Error al generar archivo flexible: {e}")
            raise
    
    def generar_md5(self, file_path, output_path):
        """
        Calcula y guarda el hash MD5 de un archivo.
        
        Args:
            file_path (str): Ruta del archivo a calcular el hash.
            output_path (str): Ruta donde se guardará el hash MD5.
        
        Raises:
            Exception: Si ocurre un error durante el cálculo del MD5.
        """
        try:
            logger.info(f"🔍 Calculando MD5 para el archivo: {file_path}")
            file_hash = hashlib.md5()
            df = self.spark.read.text(file_path)
            
            for row in df.select(col("value")).toLocalIterator():
                file_hash.update(row.value.encode('utf-8'))
            
            md5_result = file_hash.hexdigest()
            dbutils.fs.put(output_path, md5_result, True)
            
            logger.info(f"✅ MD5 generado y guardado en: {output_path}")
        except Exception as e:
            logger.error(f"❌ Error al calcular MD5: {e}")
            raise