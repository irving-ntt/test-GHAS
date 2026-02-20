from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame  # PySpark estándar
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame  # PySpark Connect
from logger import logger
import gc  # Recolector de basura de Python

class CleanUpManager:
    """
    Clase para limpiar caché de Spark y eliminar DataFrames creados en el notebook actual.
    """

    @staticmethod
    def cleanup_notebook(context):
        """
        Limpia la caché de Spark y borra DataFrames **del notebook actual** (soporta PySpark y PySpark Connect).
        También ejecuta `gc.collect()` para liberar memoria en Python.
        Identifica DataFrames que no pudieron ser eliminados porque la tabla de origen ya no existe.
        """
        # 🔹 USAR la sesión existente en lugar de crear una nueva
        from modules import DBXConnectionManager
        dbx_manager = DBXConnectionManager.get_instance()
        spark = dbx_manager.spark  # Reutilizar la sesión existente
        #spark = SparkSession.builder.getOrCreate()
        deleted_items = []
        not_deleted_items = []
        df_count = 0

        # 🔹 1. Limpiar caché de Spark
        try:
            spark.catalog.clearCache()
            deleted_items.append("Caché de Spark")
        except Exception:
            pass  # No agregamos detalles de error

        # 🔹 2. Obtener variables del notebook
        try:
            # Buscar los DataFrames en el contexto recibido (PySpark y PySpark Connect)
            df_vars = {
                var_name: var
                for var_name, var in context.items()
                if isinstance(var, (SparkDataFrame, ConnectDataFrame))  # ✅ Soporta ambos tipos
            }

            # Eliminar cada DataFrame encontrado
            for var_name, df in df_vars.items():
                try:
                    df.unpersist()
                    context.pop(var_name, None)  # Eliminamos el DataFrame del contexto del notebook
                    df_count += 1
                    deleted_items.append(var_name)
                except Exception as e:
                    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                        not_deleted_items.append(var_name)  # Agregar a lista de no eliminados
                    pass  # No agregamos detalles de error

        except Exception:
            pass  # No agregamos detalles de error

        # 🔹 3. Ejecutar el recolector de basura para liberar memoria en Python
        gc.collect()

        # 🔹 4. Construcción del resumen final
        summary = f"📌 Limpieza completada: {df_count} DataFrame(s) eliminado(s), Caché de Spark liberada."
        
        if deleted_items:
            summary += f"\n🗑 Eliminados: {', '.join(deleted_items)}"
        if not_deleted_items:
            summary += f"\n⚠️ No eliminados (tabla no encontrada): {', '.join(not_deleted_items)}"

        logger.info(summary)