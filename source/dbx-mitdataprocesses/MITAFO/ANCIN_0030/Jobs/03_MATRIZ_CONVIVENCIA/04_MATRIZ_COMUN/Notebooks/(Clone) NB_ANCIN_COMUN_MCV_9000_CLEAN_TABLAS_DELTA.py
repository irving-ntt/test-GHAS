# Databricks notebook source
"""
Descripcion:
    Identifica las tabla delta del proceso y las elimina
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN - 
    Tablas input:
    CIERREN.TFAFOGRAL_CONFIG_CONVIV
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
    CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
    CIERREN.TTCRXGRAL_FOLIO
    CIERREN.TRAFOGRAL_MOV_SUBCTA
    CIERREN.TFAFOGRAL_CONFIG_CONCEP_MOV
    CIERREN.TFAFOGRAL_CONFIG_SUBPROCESO
Tablas output:
    N/A 
Tablas Delta:
    TEMP_DELTA_COMUN_MCV_01_PRE_MAT_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_02_PRE_MAT_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_03_CONF_CONV_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_01_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_01_VAL_SUF_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_02_NO_CONVIV_{params.sr_folio}
    TEMP_DELTA_COMUN_MCV_DELTA_03_VAL_CARGO_{params.sr_folio}

Archivos SQL:
    COMUN_MCV_100_DB_0200_EXT_INFO_MATRIZ_CON_MARCA.sql
    COMUN_MCV_100_DB_0300_EXT_INFO_CONFIG_CONVIV.sql
    COMUN_MCV_100_TD_0100_GEN_MATRIZ_DELTA_01.sql
    COMUN_MCV_100_TD_0200_GEN_MATRIZ_DELTA_01_VAL_SUF.sql
    COMUN_MCV_100_TD_0300_GEN_MATRIZ_DELTA_02_NO_CONVIV.sql
    COMUN_MCV_100_TD_0400_GEN_MATRIZ_DELTA_03_VAL_CARGO.sql
    COMUN_MCV_110_TD_01_APP_DELTA_VAL_CARGO_A_VAL_SUF.sql


    
    """

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

conf = ConfManager()


# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_tipo_mov": str,
    "sr_conv_ingty": str,
    "sr_path_arch": str,
    "sr_tipo_ejecucion": str,
    # Agregamos valores faltantes para notificacion
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

# Notify.send_notification("INFO", params)


# COMMAND ----------

from pyspark.sql.functions import col

Tablas_Persistentes = []

# List delta tables with the specified folio
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("temp_delta_comun_mcv")) \
    .filter(col("tableName").endswith(params.sr_folio)) \
    .filter(~col("tableName").isin(Tablas_Persistentes))

display(tables)

# COMMAND ----------

# DBTITLE 1,Elimina Tablas DELTA - INCO
from pyspark.sql.functions import col

Tablas_Persistentes = []

# List delta tables with the specified folio
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("temp_delta_comun_mcv")) \
    .filter(col("tableName").endswith(params.sr_folio)) \
    .filter(~col("tableName").isin(Tablas_Persistentes))

display(tables)

# Drop the tables with the specified folio
for row in tables.collect():
    table_name = row.tableName
    try: 
        spark.sql(f"DROP TABLE IF EXISTS {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{table_name}")
        print(f"tabla {table_name} borrada")
    except:
        print(f"Error al borrar: {table_name} - Permisos insuficientes") 


# Forzar la recolección de basura
import gc
gc.collect()

print("Proceso de eliminación de tablas y liberación de memoria completado.")

# COMMAND ----------

# MAGIC %md
# MAGIC Las celdas siguientes no ten

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import Row

Tablas_Persistentes = []

# 1. Listar tablas filtradas
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("temp_delta_comun_mcv")) \
    .filter(col("tableName").endswith(params.sr_folio)) \
    .filter(~col("tableName").isin(Tablas_Persistentes))

# 2. Obtener la lista de nombres de tabla
table_names = [row['tableName'] for row in tables.collect()]

# 3. Para cada tabla, obtener detalles con DESCRIBE DETAIL
details = []
for table_name in table_names:
    full_table_name = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{table_name}"
    detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
    row = detail_df.select("name", "createdAt", "lastModified").first()
    # Puedes agregar más columnas si lo deseas, como 'location', 'sizeInBytes', etc.
    details.append(Row(tableName=row['name'], createdAt=row['createdAt'], lastModified=row['lastModified']))

# 4. Crear un DataFrame con los detalles
if details:
    details_df = spark.createDataFrame(details)
    display(details_df)
else:
    print("No hay tablas que cumplan los criterios.")


# COMMAND ----------

# DBTITLE 1,Eliminacion 2a version
from datetime import datetime
from pyspark.sql import Row

eliminacion_log = []

for row in details_df.collect():
    table_name = row['tableName']
    full_table_name = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{table_name}"
    fecha_eliminacion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        exito = True
        print(f"Eliminada exitosamente: {full_table_name}")
    except Exception as e:
        exito = False
        print(f"Error al eliminar {full_table_name}: {e}")
    eliminacion_log.append(Row(
        tableName=table_name,
        fechaEliminacion=fecha_eliminacion,
        exito=exito
    ))

# Crear un DataFrame con el registro de eliminación
if eliminacion_log:
    eliminacion_df = spark.createDataFrame(eliminacion_log)
    display(eliminacion_df)
else:
    print("No se intentó eliminar ninguna tabla.")

