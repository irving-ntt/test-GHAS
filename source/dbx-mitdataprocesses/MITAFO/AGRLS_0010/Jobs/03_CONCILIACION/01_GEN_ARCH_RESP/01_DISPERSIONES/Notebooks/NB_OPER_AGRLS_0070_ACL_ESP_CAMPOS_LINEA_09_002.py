# Databricks notebook source
'''
Descripcion:
    Unificación de la linea 2, 3 y 9 para el guardado del archivo de respuesta 
Subetapa:
    20 - ARCHIVO RESPUESTA
Trámite:
    118 - DAEIMSS
    120 - DOISSSTE
    122 - DITISSSTE
Tablas INPUT:
    N/A
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_ARCH_RESP_#SR_ID_ARCHIVO#
    DELTA_ACLARACIONES_ESPECIALES_JOIN_300_LEE_ARCH_#SR_ID_ARCHIVO#
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    ACLARACIONES_ESPECIALES_010.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio" : str,
    "sr_id_archivo": str,
    "sr_fec_arc" : str,
    "sr_fec_liq" : str,
    "sr_dt_org_arc" : str,
    "sr_origen_arc" : str,
    "sr_tipo_layout": str,
    "sr_tipo_reporte" : str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    })
# Validar widgets
params.validate()

# COMMAND ----------

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

file_manager = FileManager(err_repo_path=conf.err_repo_path)

# COMMAND ----------

DELTA_TABLE_ARCH_RESP = "DELTA_ACLARACIONES_ESPECIALES_ARCH_RESP_" + params.sr_id_archivo

# COMMAND ----------

DELTA_TABLE_001 = "DELTA_ACLARACIONES_ESPECIALES_JOIN_300_LEE_ARCH_" + params.sr_id_archivo

DF_JOIN_300_LEE_ARCH = db.read_delta(DELTA_TABLE_001)

DF_JOIN_300_LEE_ARCH = DF_JOIN_300_LEE_ARCH.select('FTC_LINEA')

# COMMAND ----------

# DBTITLE 1,VALIDACION 01
#Ejecuta consulta para el 01
statement_001 = query.get_statement(
    "ACLARACIONES_ESPECIALES_010.sql",
    DELTA_TABLE_ACL_ESP_ARCH_RESP=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_ARCH_RESP}",
    SR_SUBPROCESO = params.sr_subproceso
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

# DELTA_TABLE_001 = "DELTA_ACLARACIONES_ESPECIALES_JOIN_300_LEE_ARCH_" + params.sr_id_archivo

# DF_JOIN_300_LEE_ARCH = db.read_delta(DELTA_TABLE_001)

# DF_JOIN_300_LEE_ARCH = DF_JOIN_300_LEE_ARCH.select('FTC_LINEA')

# COMMAND ----------

df_final = DF_JOIN_300_LEE_ARCH.union(df)

# COMMAND ----------

if conf.debug:
    display(df_final)

# COMMAND ----------

from pyspark.sql.functions import concat, lit

# Agrega un salto de línea (puedes usar '\n' para LF o '\r\n' para CRLF)
df_final = df_final.withColumn("FTC_LINEA", concat(col("FTC_LINEA"), lit("\r")))  # Cambia '\n' por '\r\n' si necesitas CRLF


# COMMAND ----------

main_path = conf.get("salida_path")
DISPERCION_INTERESES_FECHA = params.sr_fec_arc[2:4] + params.sr_fec_arc[4:6] + params.sr_fec_arc[6:8]

# COMMAND ----------

# La ruta del archivo se construye con rt_path_procesar + file_nm_pre +sr_fecha + file_nm_post + ext_arc_dat
# La fecha siempre viene vacia y las ruta del path se define como variable del sistema.
file_name  = f"{main_path}PREFT.DP.A01534.CONFAC.GDG.DAT"

if params.sr_subproceso == '120':
    file_name  = f"{main_path}PIRFT.DP.A01534.F{DISPERCION_INTERESES_FECHA}.CONFRET.GDG.DAT"
elif params.sr_subproceso == '122':
    file_name  = f"{main_path}PIRFT.DP.A01534.F{DISPERCION_INTERESES_FECHA}.CONFTRANS.GDG.DAT"

# COMMAND ----------

if len(df_final.take(1)) == 1:
    # Define la ruta final donde quieres guardar el archivo .dat
    final_file_path = SETTINGS.GENERAL.EXTERNAL_LOCATION + file_name

# 1. Guarda el DataFrame como texto en una carpeta temporal
    temp_path = final_file_path.replace('.DAT', '_temp')  # Cambia la extensión para evitar conflictos
    df_final.coalesce(1).write.mode("overwrite").text(temp_path)

# 2. Mueve y renombra el archivo que se genera
# La ruta del archivo generado señalado por el sistema será algo así como temp_path/part-00000-...
    import glob

# Busca el archivo en la carpeta temporal
    import os

    # Listar archivos generados en la carpeta temporal
    temp_files = dbutils.fs.ls(temp_path)

    # Verificamos si hay archivos y movemos el primero encontrado
    if temp_files:
        for file_info in temp_files:
            # Asegúrate de que solo mueves el archivo correcto
            if file_info.name.endswith(".txt"):
                dbutils.fs.mv(file_info.path, final_file_path)
                break

    # 3. Elimina la carpeta temporal que se creó
    dbutils.fs.rm(temp_path, recurse=True)
