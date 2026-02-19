# Databricks notebook source
'''
Descripcion:
    Creación de archivo CTINDI para Devolución de Pago sin Justificación Legal | Esta notebook corresponde a la estracción de los jobs: JP_PATRIF_CTINDI_0020_GEN_ARCHIVO_VIV97
Subetapa:
    GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      3832 - Devolución de Pago sin Justificación Legal

Tablas INPUT:
    CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    CTINDI_DPSJL_EXT_0700_OCI_GEN_ARCHIVO.sql
    CTINDI_DPSJL_TRN_0500_DBK.sql
    CTINDI_DPSJL_TRN_0800_DBK_TRAN_GEN_ARCHIVO.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    #"sr_mask_rec_trp": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Extracción para generar el primer archivo
#se hace el join con el campo 001

statement = query.get_statement(
    "CTINDI_DPSJL_EXT_0700_OCI_GEN_ARCHIVO.sql",
    sr_folio=params.sr_folio,

)

db.write_delta(f"DELTA_CTINDI_08_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_08_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Join con base


statement = query.get_statement(
    "CTINDI_DPSJL_TRN_0500_DBK.sql",
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_08_{params.sr_folio}",
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_02_{params.sr_folio}",
    sr_clave_ent_orig = '002'

)

db.write_delta(f"DELTA_CTINDI_09_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_09_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Concatenado final


statement = query.get_statement(
    "CTINDI_DPSJL_TRN_0800_DBK_TRAN_GEN_ARCHIVO.sql",
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_09_{params.sr_folio}",

)

db.write_delta(f"DELTA_CTINDI_10_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_10_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Validación de archivo vacío
if db.read_delta(f"DELTA_CTINDI_10_{params.sr_folio}").limit(1).count() > 0:
    from datetime import datetime
    import pytz

    # Inicializa la clase para subir los archivos
    file_manager = FileManager(err_repo_path=conf.err_repo_path)

    fecha_actual = dbutils.jobs.taskValues.get(taskKey="NB_OPER_ATRAN_GAR_COND", key="sr_fecha")

    # Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
    # Configuramos el nombre de acuerdo al subproceso ejecutado
    # para este archivo el parametro sec_lote debe ser: 153
    nombre_ctindi = '/DEVP1_'
    sec_lote = '163'

    full_file_name = (
            conf.external_location
            + conf.err_repo_path
            + nombre_ctindi
            + fecha_actual
            + "_"
            + sec_lote
            + ".DAT"
        )

    if conf.debug:
        display(full_file_name)

    # Genera el archivo y decide si calcular MD5 o no
    file_manager.generar_archivo_ctindi(
        df_final=db.read_delta(f"DELTA_CTINDI_10_{params.sr_folio}"),  # DataFrame que se va a guardar
        full_file_name=full_file_name,
        header=conf.header,
        calcular_md5=True  # Cambiar a False si no se requiere calcular el MD5
    )
