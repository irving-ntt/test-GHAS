# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Realiza la extracción inicial para el subproceso de Reverso de Registros
# MAGIC Subetapa:
# MAGIC     REVREG - Reverso de Registros
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC Tablas INPUT:
# MAGIC     PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
# MAGIC     CIERREN.TMSISGRAL_MAP_NCI_ITGY
# MAGIC Tablas OUTPUT:
# MAGIC     CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA_AUX
# MAGIC Tablas INPUT DELTA:
# MAGIC     N/A
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     DELTA_100
# MAGIC     DELTA_INDICADORES
# MAGIC Archivos SQL:
# MAGIC     SQL_100_MARCA_DESMARCA.sql
# MAGIC     SQL_DELTA_101_ARCHIVO.sql
# MAGIC     SQL_DELTA_102_INDICADORES.sql
# MAGIC     SQL_DELTA_103_MAT_CONV.sql
# MAGIC     SQL_UPD_MATRIZ_CONVIVENCIA.sql
# MAGIC

# COMMAND ----------

# DBTITLE 1,INICIO - CONFIGURACIONES PRINCIPALES
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,PARAMETROS
# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({

    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_proceso": str,
    "sr_subetapa": str,
    "sr_dt_org_arc": str,
    "sr_origen_arc": str,
    "sr_subproceso": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
})

# Validar widgets
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()


# COMMAND ----------

# DBTITLE 1,VALIDA EXISTENCIA SQL

queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
if conf.debug:
    display(
        queries_df.filter(
            col("Archivo SQL").startswith("SQL_100")
        )
    )

# COMMAND ----------

# DBTITLE 1,CONSTRUCCION QUERY Y LLENADO DE DELTA_100
statement_1 = query.get_statement(
    "SQL_100_MARCA_DESMARCA.sql",
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
    hints="/*+ PARALLEL(4) */"
)

#Delta DEFINICION DEPENDE DEL SUBPROCESO
db.write_delta(
    f"DELTA_100_{params.sr_folio}",
    db.read_data("default", statement_1), 
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"DELTA_100_{params.sr_folio}"))



# COMMAND ----------

# DBTITLE 1,GENERA ARCHIVO INTEGRITY
statement_2 = query.get_statement(
    "SQL_DELTA_101_ARCHIVO.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_100_{params.sr_folio}"
)

df_archivo = db.sql_delta(statement_2)

if conf.debug:
    display(df_archivo)

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df_archivo,  # DataFrame que se va a guardar
    full_file_name=conf.external_location + "/NCI/INTEGRITY/PPA_RCAT_069_" + params.sr_folio + "_3.dat",
    header=conf.header,
    calcular_md5=False  # Cambia a False si no quieres calcular el MD5
)


# COMMAND ----------

# DBTITLE 1,GENERA DELTA INDICADORES
statement_3 = query.get_statement(
    "SQL_DELTA_102_INDICADORES.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_100_{params.sr_folio}",
    SR_FOLIO=params.sr_folio
)

db.write_delta(
    f"DELTA_INDICADORES_{params.sr_folio}", 
    db.sql_delta(statement_3), 
    "overwrite"
)

if conf.debug:
    display(db.read_delta(f"DELTA_INDICADORES_{params.sr_folio}"))



# COMMAND ----------

# DBTITLE 1,GENERA CARGA MATRIZ CONVIVENCIA AUX
statement_4 = query.get_statement(
    "SQL_DELTA_103_MAT_CONV.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_100_{params.sr_folio}"
)

#SE ELIMINA PARA QUE NO SE GENERE LA DELTA
#db.write_delta(
#    f"DELTA_MAT_CONV_{params.sr_folio}", 
#    db.sql_delta(statement_4), 
#    "overwrite"
#)

#if conf.debug:
#    display(db.read_delta(f"DELTA_MAT_CONV_{params.sr_folio}"))

#  ESTO VA A INSERTAR DIRECTAMENTE A LA TABLA PARA NO USAR LA DELTA
df_100_insert = db.sql_delta(statement_4)
#db.write_data(df_100_insert, "CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA_AUX", "default", "append")
db.write_data(df_100_insert, "CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX", "default", "append")

if conf.debug:
    display(df_100_insert)


# COMMAND ----------

# DBTITLE 1,UPD MATRIZ_CONVIVENCIA
statement_upd_001 = query.get_statement(
        "SQL_UPD_MATRIZ_CONVIVENCIA.sql",
        SR_FOLIO=params.sr_folio
    )

if conf.debug:
    display(statement_upd_001)

execution = db.execute_oci_dml(
    statement=statement_upd_001, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Clean Up
CleanUpManager.cleanup_notebook(locals())
