# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Realiza la Actualización de la tabla Matriz de Convivencia
# MAGIC Subetapa:
# MAGIC     REVREG - Reverso de Registros
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC Tablas INPUT:
# MAGIC     CIERREN.TTAFOGRAL_IND_CTA_INDV_AUX
# MAGIC Tablas OUTPUT:
# MAGIC     CIERREN.TTAFOGRAL_IND_CTA_INDV
# MAGIC Tablas INPUT DELTA:
# MAGIC     DELTA_CUENTAS
# MAGIC     DELTA_INDICADORES
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     N/A
# MAGIC Archivos SQL:
# MAGIC     SQL_200_IND_CTA_INDV.sql
# MAGIC     SQL_JOIN.sql
# MAGIC     SQL_UPD_IND_CTA_INDV.sql
# MAGIC     SQL_DEL_MATRIZ_CONVIVENCIA_AUX.sql
# MAGIC     SQL_DEL_IND_CTA_INDV_AUX.sql
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
            col("Archivo SQL").startswith("SQL_200")
        )
    )

# COMMAND ----------

# DBTITLE 1,CONSTRUCCION QUERY Y LLENADO DE DELTA_CUENTAS
statement_1 = query.get_statement(
    "SQL_200_IND_CTA_INDV.sql",
    hints="/*+ PARALLEL(4) */"
)

#Delta DEFINICION DEPENDE DEL SUBPROCESO
db.write_delta(
    f"DELTA_CUENTAS_{params.sr_folio}",
    db.read_data("default", statement_1), 
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"DELTA_CUENTAS_{params.sr_folio}"))



# COMMAND ----------

# DBTITLE 1,GENERA CARGA IND_CTA_INDV_AUX
statement_2 = query.get_statement(
    "SQL_JOIN.sql",
    DELTA_INDICADORES=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INDICADORES_{params.sr_folio}",
    DELTA_CUENTAS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CUENTAS_{params.sr_folio}"
)

#ESTO SE ELIMINA PARA ESCRIBIR DIRECTAMENTE A LA TABLA AUXILIAR
#db.write_delta(
#    f"DELTA_200_{params.sr_folio}",
#    db.sql_delta(statement_2), 
#    "overwrite"
#)
#if conf.debug:
#    display(db.read_delta(f"DELTA_200_{params.sr_folio}"))

#ESTO VA EN LUGAR DE INSERTAR A LA DELTA
df_200_insert = db.sql_delta(statement_2)
#db.write_data(df_200_insert, "CIERREN.TTAFOGRAL_IND_CTA_INDV_AUX", "default", "append")
db.write_data(df_200_insert, "CIERREN_DATAUX.TTAFOGRAL_IND_CTA_INDV_AUX", "default", "append")

if conf.debug:
    display(df_200_insert)


# COMMAND ----------

# DBTITLE 1,UPD IND_CTA_INDV
statement_upd_002 = query.get_statement(
        "SQL_UPD_IND_CTA_INDV.sql",
        SR_FOLIO=params.sr_folio
    )

if conf.debug:
    display(statement_upd_002)

execution = db.execute_oci_dml(
    statement=statement_upd_002, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,BORRA TABLA AUXILIAR MATRIZ CONVIVENCIA
statement_del_001 = query.get_statement(
        "SQL_DEL_MATRIZ_CONVIVENCIA_AUX.sql",
        SR_FOLIO=params.sr_folio
    )

if conf.debug:
    display(statement_del_001)

execution = db.execute_oci_dml(
    statement=statement_del_001, async_mode=False
)


# COMMAND ----------

# DBTITLE 1,BORRA TABLA AUXILIAR IND_CTA_INDV
statement_del_002 = query.get_statement(
        "SQL_DEL_IND_CTA_INDV_AUX.sql",
        SR_FOLIO=params.sr_folio
    )

if conf.debug:
    display(statement_del_002)

execution = db.execute_oci_dml(
    statement=statement_del_002, async_mode=False
)


# COMMAND ----------

# DBTITLE 1,Notificación
Notify.send_notification("INFO", params)



# COMMAND ----------

# DBTITLE 1,Clean Up
CleanUpManager.cleanup_notebook(locals())
