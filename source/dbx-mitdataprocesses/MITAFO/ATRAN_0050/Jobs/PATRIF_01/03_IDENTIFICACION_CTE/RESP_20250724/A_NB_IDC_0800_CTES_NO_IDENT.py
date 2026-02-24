# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Genera archivo CSV con información de clientes con error
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC Tablas INPUT:
# MAGIC     CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE
# MAGIC 	CIERREN.TCCRXGRAL_TIPO_CAT 
# MAGIC 	CIERREN.TCCRXGRAL_CAT_CATALOGO
# MAGIC Tablas OUTPUT:
# MAGIC     PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
# MAGIC Tablas INPUT DELTA:
# MAGIC     N/A
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     N/A
# MAGIC Archivos SQL:
# MAGIC     SQL_IDC_800_CTES_NO_IDENT.sql
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
            col("Archivo SQL").startswith("SQL_IDC_800")
        )
    )


# COMMAND ----------

# DBTITLE 1,EJECUTA QUERY PARA GENERAR DELTA DUPLICADOS
statement_1 = query.get_statement(
    "SQL_IDC_800_CTES_NO_IDENT.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_1)

if conf.debug:
    display(df)




# COMMAND ----------

# DBTITLE 1,GENERA ARCHIVO CSV DE CLIENTES CON ERROR

# Inicializa la clase para subir los archivos
file_manager = FileManager(err_repo_path=conf.err_repo_path)

# Genera el archivo y decide si calcular MD5 o no
file_manager.generar_archivo_ctindi(
    df_final=df,  # DataFrame que se va a guardar
    full_file_name=conf.external_location + "/RCTRAS/INF/" + params.sr_folio + "_Identificacion_Cliente.CSV",
    header=conf.header,
    calcular_md5=False  # Cambia a False si no quieres calcular el MD5
)



# COMMAND ----------

# DBTITLE 1,Notificación
Notify.send_notification("INFO", params)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
