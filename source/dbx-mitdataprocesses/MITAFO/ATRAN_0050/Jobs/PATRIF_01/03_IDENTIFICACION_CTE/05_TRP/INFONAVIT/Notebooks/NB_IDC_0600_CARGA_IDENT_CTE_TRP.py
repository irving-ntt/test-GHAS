# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Inserta información en tabla OCI
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC Tablas INPUT:
# MAGIC     N/A
# MAGIC Tablas OUTPUT:
# MAGIC     N/A
# MAGIC Tablas INPUT DELTA:
# MAGIC     DELTA_CARGA
# MAGIC 	DELTA_VARIASCTAS
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     DELTA_BUC_DATOS
# MAGIC Archivos SQL:
# MAGIC     SQL_IDC_600_DELETE.sql
# MAGIC 	SQL_IDC_600_BUC_DATOS.sql
# MAGIC 	SQL_IDC_600_INSERT_IDENT.sql
# MAGIC 	SQL_IDC_600_VARIAS_CTAS.sql
# MAGIC 	SQL_IDC_600_CARGA.sql
# MAGIC 	SQL_IDC_600_DUP.sql

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
            col("Archivo SQL").startswith("SQL_IDC_600")
        )
    )

# COMMAND ----------

# DBTITLE 0,APLICA REGLAS SOBRE DELTA CARGA
statement_1 = query.get_statement(
    "SQL_IDC_600_CARGA.sql",
    DELTA_600_CARGA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CARGA_{params.sr_folio}"
)

df_carga_600 = db.sql_delta(statement_1)

if conf.debug:
    display(df_carga_600)

# COMMAND ----------

# DBTITLE 1,APLICA REGLAS SOBRE DELTA VARIASCTAS
statement_2 = query.get_statement(
    "SQL_IDC_600_VARIAS_CTAS.sql",
    DELTA_600_VARIASCTAS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_VARIASCTAS_{params.sr_folio}"
)

df_variasctas_600 = db.sql_delta(statement_2)

if conf.debug:
    display(df_variasctas_600)


# COMMAND ----------

# DBTITLE 1,UNE SALIDAS DE CARGA Y VARIASCTAS
df_union_600 = df_carga_600.union(df_variasctas_600)

if conf.debug:
    display(df_union_600)

db.write_delta(f"DELTA_600_CARGA_{params.sr_folio}", df_union_600, "overwrite")


# COMMAND ----------

# DBTITLE 1,GENERA SALIDA DELTA_BUC_DATOS
statement_3 = query.get_statement(
    "SQL_IDC_600_BUC_DATOS.sql",
    DELTA_600_BUC=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_600_CARGA_{params.sr_folio}"
)

df_600_buc = db.sql_delta(statement_3)

if conf.debug:
    display(df_600_buc)

db.write_delta(f"DELTA_BUC_DATOS_{params.sr_folio}", df_600_buc, "overwrite")


# COMMAND ----------

# DBTITLE 1,QUERY QUE GENERA LA SALIDA A INSERTAR EN OCI
statement_4 = query.get_statement(
    "SQL_IDC_600_INSERT_IDENT.sql",
    DELTA_600_INSERT=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_600_CARGA_{params.sr_folio}"
)

df_600_insert = db.sql_delta(statement_4)

if conf.debug:
    display(df_600_insert)

# COMMAND ----------

# DBTITLE 1,BORRADO PREVIO DE LA TABLA TTSISGRAL_ETL_VAL_IDENT_CTE
statement_delete = query.get_statement(
    "SQL_IDC_600_DELETE.sql",
    #TTSISGRAL_ETL_MOVIMIENTOS_AUX_OR_MAIN=table_name_aux,
    SR_FOLIO=params.sr_folio
)
execution = db.execute_oci_dml(
    statement=statement_delete, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,INSERTA EN LA TABLA TTSISGRAL_ETL_VAL_IDENT_CTE
df_600_insert = df_600_insert.withColumn(
    "FTD_FECHA_CERTIFICACION",
    #to_date(df_600_insert["FTD_FECHA_CERTIFICACION"], "yyyy-MM-dd")
    to_date(df_600_insert["FTD_FECHA_CERTIFICACION"], "dd/MM/yyyy")
)

db.write_data(df_600_insert, "CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE", "default", "append")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
