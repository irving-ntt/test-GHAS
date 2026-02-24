# Databricks notebook source
'''
Descripcion:
    Proceso que realiza las operaciones para actualizar las cifras que se insertarán en las tablas finales
Subetapa:
    26 - Cifras Control
Trámite:
    364 - Transferencias de Acreditados Infonavit
    365 - Transferencias por Anualidad Garantizada
    368 - Uso de Garantía por 43 BIS
Tablas input:
    PROCESOS.TTCRXGRAL_TRANS_INFONA
    CIERREN.TRAFOGRAL_MOV_SUBCTA
Tablas output:
    N/A
Tablas delta:
    DELTA_JOIN_400_{params.sr_id_archivo}
Archivos SQL:
    100_PROC_CONTEO_ACT.sql
    TRANS_200_TI_JOIN_ACT.sql
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

table_001 = "TTAFOTRAS_SUM_ARCHIVO_TRANS"

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

statement_tf_sumarch = query.get_statement(
    "TRANS_200_TI_JOIN_ACT_SUMARCH.sql",
    DELTA_JOIN_400=f"DELTA_JOIN_400_{params.sr_id_archivo}",
    SR_USER = params.sr_usuario
)

df_sumarch = db.sql_delta(statement_tf_sumarch)

# COMMAND ----------

if df_sumarch:
    records = df_sumarch.collect()

    for row in records:
        row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)

        sentencia_update = (
            f"UPDATE CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS SET "
            f"FTN_IMPORTE_RECH_PESOS_PRO = {valor_sql('FTN_IMPORTE_RECH_PESOS_PRO', row_dict.get('FTN_IMPORTE_RECH_PESOS_PRO'))}, "
            f"FTN_IMPORTE_RECH_AIVS_PRO = {valor_sql('FTN_IMPORTE_RECH_AIVS_PRO', row_dict.get('FTN_IMPORTE_RECH_AIVS_PRO'))}, "
            f"FTN_IMPORTE_ACT_PESOS_PRO = {valor_sql('FTN_IMPORTE_ACT_PESOS_PRO', row_dict.get('FTN_IMPORTE_ACT_PESOS_PRO'))}, "
            f"FTN_NUM_REG_RECH_PRO = {valor_sql('FTN_NUM_REG_RECH_PRO', row_dict.get('FTN_NUM_REG_RECH_PRO'))}, "
            f"FTN_NUM_REG_ACT_PRO = {valor_sql('FTN_NUM_REG_ACT_PRO', row_dict.get('FTN_NUM_REG_ACT_PRO'))}, "
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

if params.sr_tipo_archivo == '09':
    statement_tf_conteo_rech = query.get_statement(
    "TRANS_200_TI_JOIN_ACT_CONTEO_RECH.sql",
    DELTA_JOIN_400=f"DELTA_JOIN_400_{params.sr_id_archivo}"
    )
    
    df_conteo_rech = db.sql_delta(statement_tf_conteo_rech)

    if df_conteo_rech:
        records = df_conteo_rech.collect()

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

if params.sr_tipo_archivo == '06':
    statement_tf_conteo = query.get_statement(
    "TRANS_200_TI_JOIN_ACT_CONTEO.sql",
    DELTA_JOIN_400=f"DELTA_JOIN_400_{params.sr_id_archivo}"
    )
    
    df_conteo = db.sql_delta(statement_tf_conteo)

    if df_conteo_rech:
        records = df_conteo_rech.collect()

        for row in records:
            row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)

            sentencia_update = (
                f"UPDATE CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS SET "
                f"FTN_NUM_REG_ACT_PRO = {valor_sql('FTN_NUM_REG_ACT_PRO', row_dict.get('FTN_NUM_REG_ACT_PRO'))}"
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
