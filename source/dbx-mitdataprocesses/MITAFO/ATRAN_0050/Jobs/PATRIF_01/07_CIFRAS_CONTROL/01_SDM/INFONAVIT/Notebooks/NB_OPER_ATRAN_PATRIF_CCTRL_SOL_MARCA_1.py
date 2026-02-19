# Databricks notebook source
"""
Descripcion:
    Generación de Cifras Control para Solicitud de Marca 43 BIS
Subetapa: 
    26 - Cifras Control
Trámite:
    354 - IMSS Solicitud de marca de cuentas por 43 bis
Tablas input:
    CIERREN.TTSISGRAL_SUF_SALDOS
    CIERREN.TTCRXGRAL_MARCA_DESMARCA_INFO
Tablas output:
    CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS
Tablas Delta:
    DELTA_100_MARCA_DESMARCA_RECH_{params.sr_folio}   
Archivos SQL:
    100_MARCA_DESMARCA_INFO_RECH.sql
"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso":str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_etapa":str,
    "sr_id_archivo": str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_id_snapshot":str,
    "sr_recalculo":str,
    "sr_tipo_archivo":str,
    "sr_tipo_layout":str,
})
# Validar widgets
# params.validate()

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "100_MARCA_DESMARCA_INFO_RECH.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    TIPO_ARCH= params.sr_tipo_archivo,
    SR_USUARIO = params.sr_usuario,
)

df = db.read_data("default",statement)

DELTA_TABLE_001 = f"DELTA_100_MARCA_DESMARCA_RECH_{params.sr_folio}"
db.write_delta(DELTA_TABLE_001,df , "overwrite")

# COMMAND ----------

if conf.debug:
    display(df)

# COMMAND ----------

def valor_sql(clave, valor):
    if valor is None:
        return "NULL"
    if clave == "FTD_FEH_ACT":
        # Usa TO_TIMESTAMP para el formato 'YYYY-MM-DD HH24:MI:SS.FF6'
        return f"TO_TIMESTAMP('{valor}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
    if isinstance(valor, str):
        return f"'{valor}'"
    return str(valor)

# COMMAND ----------

if df:
    records = df.collect()

    for row in records:
        row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)

        sentencia_update = (
            f"UPDATE CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS SET "
            f"FTN_IMPORTE_RECH_PESOS_PRO = {valor_sql('FTN_IMPORTE_RECH_PESOS_PRO', row_dict.get('FTN_IMPORTE_RECH_PESOS_PRO'))}, "
            f"FTN_IMPORTE_RECH_AIVS_PRO = {valor_sql('FTN_IMPORTE_RECH_AIVS_PRO', row_dict.get('FTN_IMPORTE_RECH_AIVS_PRO'))}, "
            f"FTN_NUM_REG_RECH_PRO = {valor_sql('FTN_NUM_REG_RECH_PRO', row_dict.get('FTN_NUM_REG_RECH_PRO'))}, "
            f"FTD_FEH_ACT = {valor_sql('FTD_FEH_ACT', row_dict.get('FTD_FEH_ACT'))}, "
            f"FTC_USU_ACT = {valor_sql('FTC_USU_ACT', row_dict.get('FTC_USU_ACT'))} "
            f"WHERE FTC_FOLIO = {valor_sql('FTC_FOLIO', row_dict.get('FTC_FOLIO'))}"
        )
        db.execute_oci_dml(
            statement=sentencia_update, async_mode=False
        )
else:
    logger.info("No se encontraron registros para actualizar")

# COMMAND ----------

df_conteo = df.select(["FTC_FOLIO", "FTN_NUM_REG_RECH_PRO"])

if df_conteo:
    records = df_conteo.collect()

    for row in records:
        row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)

        sentencia_update = (
            f"UPDATE CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS SET "
            f"FTN_NUM_REG_RECH_PRO = {valor_sql('FTN_NUM_REG_RECH_PRO', row_dict.get('FTN_NUM_REG_RECH_PRO'))}"
            f"WHERE FTC_FOLIO = {valor_sql('FTC_FOLIO', row_dict.get('FTC_FOLIO'))}"
        )
        db.execute_oci_dml(
            statement=sentencia_update, async_mode=False
        )
else:
    logger.info("No se encontraron registros para actualizar")

# COMMAND ----------

# Se elimina información por folio de la tabla histórica
delete_statement = f"DELETE FROM CIERREN.THAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

# Se obtiene información de la tabla original TTAFOTRAS_SUM_ARCHIVO_TRANS y se inserta en el histórico
statement_insert = query.get_statement(
    "CCTRL_SUM_ARCHIVO_TRANS_HISTORICO.sql",
    SR_FOLIO=params.sr_folio
)

# Se inserta información en tabla de histórica
execution = db.execute_oci_dml(
        statement=statement_insert, async_mode=False
    )
