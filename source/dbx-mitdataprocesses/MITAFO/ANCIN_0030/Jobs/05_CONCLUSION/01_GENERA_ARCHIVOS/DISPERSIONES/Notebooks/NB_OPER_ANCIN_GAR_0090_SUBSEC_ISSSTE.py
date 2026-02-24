# Databricks notebook source
# DBTITLE 1,Descripción
'''
Descripcion:
    Creación de archivo CTINDI para la Dispersión Ordinaria ISSSTE Subsecuente. 
Subetapa:
    GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      120 - DISPERSIÓN ORDINARIA ISSSTE
Tablas INPUT:
    CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    ANCIN_GAR_0090_SUBSEC_ISSSTE.SQL
'''

# COMMAND ----------

# DBTITLE 1,Inicio - Configuraciones principales
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_folio": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_subetapa": str,
        "sr_sec_lote": str,
        "sr_fecha_lote": str,
        "sr_fecha_acc": str,
        "sr_tipo_archivo": str,
        "sr_estatus_mov": str,
        "sr_tipo_mov": str,
        "sr_accion": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str, # es obligatorio para servicios
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(
    queries_df.filter(
        col("Archivo SQL").startswith("ANCIN_GAR_0090_SUBSEC_ISSSTE")
    )
)

# COMMAND ----------

# DBTITLE 1,Construcción SQL
statement_001 = query.get_statement(
    "ANCIN_GAR_0090_SUBSEC_ISSSTE.sql",
    SR_FECHA_ACC=params.sr_fecha_acc,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

# DBTITLE 1,Extraccion de datos
df = db.read_data("default", statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,Validación de archivo vacío
if df.limit(1).count() == 0:
    Notify.send_notification("INFO", params)
    dbutils.notebook.exit("No hay información para generar el archivo.")

# COMMAND ----------

# DBTITLE 1,Configuraciones del archivo final
from datetime import datetime
import pytz

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Define the timezone for Mexico
mexico_tz = pytz.timezone("America/Mexico_City")

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual

full_file_name = (
        conf.external_location
        + conf.err_repo_path
        + "/PPA_RCDI02005_"
        + fecha_actual
        + "_"
        + params.sr_sec_lote
        + ".DAT"
    )
full_file_name_md5 = (
        conf.external_location
        + conf.err_repo_path
        + "/MD5_PPA_RCDI02005_"
        + fecha_actual
        + "_"
        + params.sr_sec_lote
        + ".DAT.md5"
    )

if conf.debug:
    display(full_file_name)

# COMMAND ----------

# DBTITLE 1,Creación de archivo subsec ISSSTE
# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df,  # DataFrame que se va a guardar
    full_file_name=full_file_name,
    header=conf.header,
    calcular_md5=False  # Cambiar a False si no se requiere calcular el MD5
)

# COMMAND ----------

# DBTITLE 1,Notificación de proceso finalizado
Notify.send_notification("INFO", params)

# COMMAND ----------

# DBTITLE 1,Fin - Limpieza
CleanUpManager.cleanup_notebook(locals())

# COMMAND ----------

# DBTITLE 1,Copia archivo a local
#En caso de que se requiera validar el archivo en la maquina local
#dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/PPA_RCDI02005_20250703_977.DAT', '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_cararch/')
