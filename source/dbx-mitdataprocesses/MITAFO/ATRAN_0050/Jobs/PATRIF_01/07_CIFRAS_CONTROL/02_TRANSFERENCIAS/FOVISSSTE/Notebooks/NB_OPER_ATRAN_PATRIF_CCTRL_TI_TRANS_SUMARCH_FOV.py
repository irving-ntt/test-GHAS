# Databricks notebook source
'''
Descripcion:
    Proceso que realiza el proceso previo a la inserción de las tablas finales.
Subetapa:
    26 - Cifras Control
Trámite:
    363 - Transferencia de acreditados FOVISSSTE
Tablas input:
    CIERREN.TRAFOGRAL_MOV_SUBCTA
Tablas output:
    N/A
Tablas delta:
    DELTA_TRANS_400_JOIN_{params.sr_id_archivo}
Archivos SQL:
    TF_710_SUMARCH_FOV_700.sql
    TF_710_SUMARCH_FOV_800.sql
'''

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

statement = query.get_statement(
    "TF_710_SUMARCH_FOV_700.sql",
    SR_USER = params.sr_usuario,
    DELTA_TRANS_400_JOIN=f"DELTA_TRANS_400_JOIN_{params.sr_id_archivo}"
)

result_200 = db.sql_delta(statement)

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

if result_200:
    records = result_200.collect()

    for row in records:
        row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)

        sentencia_update = (
            f"UPDATE CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS SET "
            f"FTN_IMPORTE_RECH_PESOS_PRO = {valor_sql('FTN_IMPORTE_RECH_PESOS_PRO', row_dict.get('FTN_IMPORTE_RECH_PESOS_PRO'))}, "
            f"FTN_IMPORTE_RECH_AIVS_PRO = {valor_sql('FTN_IMPORTE_RECH_AIVS_PRO', row_dict.get('FTN_IMPORTE_RECH_AIVS_PRO'))}, "
            f"FTN_NUM_REG_RECH_PRO = {valor_sql('FTN_NUM_REG_RECH_PRO', row_dict.get('FTN_NUM_REG_RECH_PRO'))}, "
            f"FTD_FEH_ACT = {valor_sql('FTD_FEH_ACT', row_dict.get('FTD_FEH_ACT'))}, "
            f"FTC_USU_ACT = {valor_sql('FTC_USU_ACT', row_dict.get('FTC_USU_ACT'))} "
            f"WHERE FTC_FOLIO = {valor_sql('FTC_FOLIO', row_dict.get('FTC_FOLIO'))} "
            f"AND FFN_ID_CONCEPTO_IMP = {valor_sql('FFN_ID_CONCEPTO_IMP', row_dict.get('FFN_ID_CONCEPTO_IMP'))}"
        )
        db.execute_oci_dml(
            statement=sentencia_update, async_mode=False
        )
else:
    logger.info("No se encontraron registros para actualizar")

# COMMAND ----------


statement_0300 = query.get_statement(
    "TF_710_SUMARCH_FOV_800.sql",
    SR_USER = params.sr_usuario,
    DELTA_TRANS_400_JOIN=f"DELTA_TRANS_400_JOIN_{params.sr_id_archivo}"
)

result_300 = db.sql_delta(statement_0300)

# COMMAND ----------

if result_300:
    records = result_300.collect()

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
