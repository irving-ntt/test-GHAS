# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

# Los únicos parámetros válidos para este notebook son los definidos aquí.
# Los parámetros dinámicos se obtienen de params, los valores constantes de conf.
params = WidgetParams(
    {
        "sr_actualiza": str,
        "sr_etapa": str,
        "sr_fec_acc": str,
        "sr_fec_liq": str,
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_paso": str,
        "sr_proceso": str,
        "sr_reproceso": str,
        "sr_subetapa": str,
        "sr_subproceso": str,
        "sr_usuario": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columna 'Archivo SQL' contenga el nombre del notebook
display(queries_df.filter(col("Archivo SQL").contains("MOV_TRANS_INFO_02")))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316505?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Lectura inicial desde tabla Delta definida en conf (Omitir este paso)
# IMPORTANTE: Las tablas Delta de Conf DEBEN tipificarse con _{params.sr_folio}
delta_table = f"{conf.delta_500_saldo}_{params.sr_folio}"
logger.info(f"Leyendo insumo inicial desde tabla Delta: {delta_table}")

statement_001 = query.get_statement(
    "NB_PATRIF_MOV_TRANS_INFO_02_001_READ_DATASET.sql",
    DELTA_TABLE=delta_table,
    SR_FOLIO=params.sr_folio,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

db.write_delta(
    f"NB_PATRIF_MOV_TRANS_INFO_02_001_{params.sr_folio}",
    db.sql_delta(statement_001),
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"NB_PATRIF_MOV_TRANS_INFO_02_001_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316506?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_A.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Traer datos de Oracle - Valor de Acción
logger.info("Trayendo datos de Oracle: TCAFOGRAL_VALOR_ACCION")
statement_002a = query.get_statement(
    "NB_PATRIF_MOV_TRANS_INFO_02_002A_READ_VALOR_ACCION.sql",
    hints="/*+ PARALLEL(4) */",
)
db.write_delta(
    f"TEMP_VALOR_ACCION_{params.sr_folio}",
    db.read_data("default", statement_002a),
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"TEMP_VALOR_ACCION_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316507?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_B.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Join con Valor de Acción (solo Delta)
statement_002b = query.get_statement(
    "NB_PATRIF_MOV_TRANS_INFO_02_002B_JOIN_VALOR_ACCION.sql",
    SR_FOLIO=params.sr_folio,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

db.write_delta(
    f"NB_PATRIF_MOV_TRANS_INFO_02_002_{params.sr_folio}",
    db.sql_delta(statement_002b),
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"NB_PATRIF_MOV_TRANS_INFO_02_002_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316509?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_C.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Traer datos de Oracle - Tipo de Subcuenta
logger.info("Trayendo datos de Oracle: TCCRXGRAL_TIPO_SUBCTA")
statement_003a = query.get_statement(
    "NB_PATRIF_MOV_TRANS_INFO_02_003A_READ_TIPO_SUBCTA.sql",
    hints="/*+ PARALLEL(4) */",
)
db.write_delta(
    f"TEMP_TIPO_SUBCTA_{params.sr_folio}",
    db.read_data("default", statement_003a),
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"TEMP_TIPO_SUBCTA_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316508?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_D.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Lookup con Tipo de Subcuenta (solo Delta)
statement_003b = query.get_statement(
    "NB_PATRIF_MOV_TRANS_INFO_02_003B_LOOKUP_TIPO_SUBCTA.sql",
    SR_FOLIO=params.sr_folio,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

db.write_delta(
    f"NB_PATRIF_MOV_TRANS_INFO_02_003_{params.sr_folio}",
    db.sql_delta(statement_003b),
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"NB_PATRIF_MOV_TRANS_INFO_02_003_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316511?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_E.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Traer datos de Oracle - Concepto de Movimiento
logger.info("Trayendo datos de Oracle: Concepto de Movimiento")
statement_004a = query.get_statement(
    "NB_PATRIF_MOV_TRANS_INFO_02_004A_READ_CONCEPTO_MOV.sql",
    SR_SUBPROCESO=params.sr_subproceso,
    hints="/*+ PARALLEL(4) */",
)
db.write_delta(
    f"TEMP_CONCEPTO_MOV_{params.sr_folio}",
    db.read_data("default", statement_004a),
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"TEMP_CONCEPTO_MOV_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316510?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_F.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Lookup con Concepto de Movimiento (solo Delta)
statement_004b = query.get_statement(
    "NB_PATRIF_MOV_TRANS_INFO_02_004B_LOOKUP_CONCEPTO_MOV.sql",
    SR_FOLIO=params.sr_folio,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

db.write_delta(
    f"NB_PATRIF_MOV_TRANS_INFO_02_004_{params.sr_folio}",
    db.sql_delta(statement_004b),
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"NB_PATRIF_MOV_TRANS_INFO_02_004_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316512?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_G.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Traer datos de Oracle - Matriz de Convivencia
if str(params.sr_subproceso) not in ("348", "349", "350"):
    logger.info("Trayendo datos de Oracle: Matriz de Convivencia")
    statement_005a = query.get_statement(
        "NB_PATRIF_MOV_TRANS_INFO_02_005A_READ_MATRIZ_CONV.sql",
        SR_FOLIO=params.sr_folio,
        SR_SUBPROCESO=params.sr_subproceso,
        hints="/*+ PARALLEL(4) */",
    )
    db.write_delta(
        f"TEMP_MATRIZ_CONV_{params.sr_folio}",
        db.read_data("default", statement_005a),
        "overwrite",
    )
    if conf.debug:
        display(db.read_delta(f"TEMP_MATRIZ_CONV_{params.sr_folio}"))
else:
    logger.info("Trayendo datos de Oracle: Matriz de Convivencia DEVO")
    statement_005a = query.get_statement(
        "NB_PATRIF_MOV_TRANS_INFO_02_005A_READ_MATRIZ_CONV_348_349.sql",
        SR_FOLIO=params.sr_folio,
        hints="/*+ PARALLEL(4) */",
    )
    db.write_delta(
        f"TEMP_MATRIZ_CONV_{params.sr_folio}",
        db.read_data("default", statement_005a),
        "overwrite",
    )
    if conf.debug:
        display(db.read_delta(f"TEMP_MATRIZ_CONV_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316513?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_H.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,Join con Matriz de Convivencia (solo Delta)
if str(params.sr_subproceso) not in ("348", "349", "350"):
    statement_005b = query.get_statement(
        "NB_PATRIF_MOV_TRANS_INFO_02_005B_JOIN_MATRIZ_CONV.sql",
        SR_FOLIO=params.sr_folio,
        CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    )

    db.write_delta(
        f"NB_PATRIF_MOV_TRANS_INFO_02_005_{params.sr_folio}",
        db.sql_delta(statement_005b),
        "overwrite",
    )
    if conf.debug:
        display(db.read_delta(f"NB_PATRIF_MOV_TRANS_INFO_02_005_{params.sr_folio}"))
else:
    statement_005b = query.get_statement(
        "NB_PATRIF_MOV_TRANS_INFO_02_005B_JOIN_MATRIZ_CONV_348_349.sql",
        SR_FOLIO=params.sr_folio,
        CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    )

    db.write_delta(
        f"NB_PATRIF_MOV_TRANS_INFO_02_005_{params.sr_folio}",
        db.sql_delta(statement_005b),
        "overwrite",
    )
    if conf.debug:
        display(db.read_delta(f"NB_PATRIF_MOV_TRANS_INFO_02_005_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Eliminar registros previos (DELETE en OCI)
logger.info("Eliminando registros previos en OCI")
statement_006a = query.get_statement(
    "NB_PATRIF_MOV_TRANS_INFO_02_006A_DELETE_PREVIOUS.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    hints="/*+ PARALLEL(8) */",
)

execution_delete = db.execute_oci_dml(statement=statement_006a, async_mode=False)

# COMMAND ----------

# MAGIC %md
# MAGIC <p style="display: flex; gap: 10px;">
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/3517236121316514?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/NB_PATRIF_MOV_TRANS_INFO_02_I.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </p>

# COMMAND ----------

# MAGIC %md
# MAGIC # Mapeo de campos
# MAGIC | Origen / Regla                                               | Destino              |
# MAGIC |--------------------------------------------------------------|----------------------|
# MAGIC | If IsNull(FCN_ID_CAT_SUBCTA) Then 0 Else FCN_ID_CAT_SUBCTA   | FCN_ID_CAT_SUBCTA    |
# MAGIC | FTC_FOLIO                                                    | FTC_FOLIO            |
# MAGIC | SetNull()                                                    | FTC_FOLIO_REL        |
# MAGIC | If IsNull(FTN_MONTO_PESOS) Then 0 Else FTN_MONTO_PESOS       | FTF_MONTO_PESOS      |
# MAGIC | If IsNull(FTN_SDO_AIVS) Then 0 Else FTN_SDO_AIVS             | FTF_MONTO_ACCIONES   |
# MAGIC | FCD_FEH_ACCION                                               | FCD_FEH_ACCION       |
# MAGIC | FTN_NUM_CTA_INVDUAL                                          | FTN_NUM_CTA_INVDUAL  |
# MAGIC | If IsNull(FTN_ID_MARCA) Then 0 Else FTN_ID_MARCA             | FTN_ID_MARCA         |
# MAGIC | p_FL_ID_TIPO_MOV                                             | FCN_ID_TIPO_MOV      |
# MAGIC | p_TABLA_NCI_VIV                                              | FCC_TABLA_NCI_VIV    |
# MAGIC | FTN_CONTA_SERV                                               | FNN_ID_REFERENCIA    |
# MAGIC | 0                                                            | FTN_PRE_MOV_GENERADO |
# MAGIC | p_SR_SUBPROCESO                                              | FCN_ID_SUBPROCESO    |
# MAGIC
# MAGIC # FTF_MONTO_PESOS_SOL
# MAGIC | Condición                        | Valor asignado                      |
# MAGIC |----------------------------------|--------------------------------------|
# MAGIC | If p_SR_SUBPROCESO = 364         | FTN_MONTO_PESOS                      |
# MAGIC | Else If p_SR_SUBPROCESO = 365    | FTN_NUM_APLI_INTE_VIV * FTN_VALOR_AIVS |
# MAGIC | Else If p_SR_SUBPROCESO = 363    | FTN_SALDO_VIV                        |
# MAGIC | Else If p_SR_SUBPROCESO = 3286   | FTN_SALDO_VIV                        |
# MAGIC | Else If p_SR_SUBPROCESO = 368    | FTN_SALDO_VIV                        |
# MAGIC | Else                             | 0                                    |
# MAGIC
# MAGIC # FTF_MONTO_ACCIONES_SOL
# MAGIC | Condición                        | Valor asignado        |
# MAGIC |----------------------------------|------------------------|
# MAGIC | If p_SR_SUBPROCESO = 364         | FTN_SDO_AIVS           |
# MAGIC | Else If p_SR_SUBPROCESO = 365    | FTN_NUM_APLI_INTE_VIV  |
# MAGIC | Else If p_SR_SUBPROCESO = 363    | FTN_NUM_APLI_INTE_VIV  |
# MAGIC | Else If p_SR_SUBPROCESO = 3286   | FTN_NUM_APLI_INTE_VIV  |
# MAGIC | Else If p_SR_SUBPROCESO = 368    | FTN_NUM_APLI_INTE_VIV  |
# MAGIC | Else                             | 0                      |
# MAGIC
# MAGIC # Campos finales
# MAGIC | Origen / Regla                                             | Destino              |
# MAGIC |------------------------------------------------------------|----------------------|
# MAGIC | FTN_DEDUCIBLE                                              | FTN_DEDUCIBLE        |
# MAGIC | FCN_ID_PLAZO                                               | FCN_ID_PLAZO         |
# MAGIC | CurrentTimestamp()                                         | FCD_FEH_CRE          |
# MAGIC | PScnxns.$CX_CRE_USUARIO                                    | FCC_USU_CRE          |
# MAGIC | SetNull()                                                  | FCD_FEH_ACT          |
# MAGIC | SetNull()                                                  | FCC_USU_ACT          |
# MAGIC | If IsNull(FFN_ID_CONCEPTO_MOV) Then 0 Else FFN_ID_CONCEPTO_MOV | FCN_ID_CONCEPTO_MOV |
# MAGIC | p_SIEFORE                                                  | FCN_ID_SIEFORE       |
# MAGIC
# MAGIC ### Reglas para sr_subproceso 348 y 349
# MAGIC | Origen / Regla                                               | Destino              |
# MAGIC |--------------------------------------------------------------|----------------------|
# MAGIC | If IsNull(FCN_ID_CAT_SUBCTA) Then 0 Else FCN_ID_CAT_SUBCTA   | FCN_ID_CAT_SUBCTA    |
# MAGIC | FTC_FOLIO                                                    | FTC_FOLIO            |
# MAGIC | SetNull()                                                    | FTC_FOLIO_REL        |
# MAGIC | If IsNull(FTN_MONTO_PESOS) Then 0 Else FTN_MONTO_PESOS       | FTF_MONTO_PESOS      |
# MAGIC | If IsNull(FTN_SDO_AIVS) Then 0 Else FTN_SDO_AIVS             | FTF_MONTO_ACCIONES   |
# MAGIC | FCD_FEH_ACCION                                               | FCD_FEH_ACCION       |
# MAGIC | FTN_NUM_CTA_INVDUAL                                          | FTN_NUM_CTA_INVDUAL  |
# MAGIC | If IsNull(FTN_ID_MARCA) Then 0 Else FTN_ID_MARCA             | FTN_ID_MARCA         |
# MAGIC | p_FL_ID_TIPO_MOV                                             | FCN_ID_TIPO_MOV      |
# MAGIC | p_TABLA_NCI_VIV                                              | FCC_TABLA_NCI_MOV    |
# MAGIC | FTN_CONTA_SERV                                               | FNN_ID_REFERENCIA    |
# MAGIC | 0                                                            | FTN_PRE_MOV_GENERADO |
# MAGIC | p_SR_SUBPROCESO                                              | FCN_ID_SUBPROCESO    |
# MAGIC | If IsNull(FTN_SALDO_VIV) Then 0 Else FTN_SALDO_VIV           | FTF_MONTO_PESOS_SOL  |
# MAGIC | If IsNull(FTN_NUM_APLI_INTE_VIV) Then 0 Else FTN_NUM_APLI_INTE_VIV | FTF_MONTO_ACCIONES_SOL |
# MAGIC | FTN_DEDUCIBLE                                                | FTN_DEDUCIBLE        |
# MAGIC | FCN_ID_PLAZO                                                 | FCN_ID_PLAZO         |
# MAGIC | CurrentTimestamp()                                           | FCD_FEH_CRE          |
# MAGIC | PScnxns.$CX_CRE_USUARIO                                      | FCC_USU_CRE          |
# MAGIC | SetNull()                                                    | FCD_FEH_ACT          |
# MAGIC | SetNull()                                                    | FCC_USU_ACT          |
# MAGIC | If IsNull(FFN_ID_CONCEPTO_MOV) Then 0 Else FFN_ID_CONCEPTO_MOV | FCN_ID_CONCEPTO_MOV |
# MAGIC | p_SIEFORE                                                    | FCN_ID_SIEFORE       |

# COMMAND ----------

# DBTITLE 1,Transformar datos para inserción (solo Delta)
logger.info("Transformando datos para inserción")

# Determinar el valor correcto de FL_ID_TIPO_MOV según el subproceso
# Los subprocesos 348, 349 y 350 corresponden a procesos de vivienda (VIV97)
# y requieren p_FL_ID_TIPO_MOV = 181 en lugar del valor por defecto (180)
if str(params.sr_subproceso) in ("348", "349", "350"):
    fl_id_tipo_mov = (
        "181"  # Valor específico para subprocesos 348, 349 y 350 (vivienda)
    )
else:
    fl_id_tipo_mov = (
        conf.p_fl_id_tipo_mov
    )  # Valor por defecto (180) para otros subprocesos

if str(params.sr_subproceso) not in ("348", "349", "350"):
    statement_006b = query.get_statement(
        "NB_PATRIF_MOV_TRANS_INFO_02_006B_TRANSFORM_DATA.sql",
        SR_FOLIO=params.sr_folio,
        SR_SUBPROCESO=params.sr_subproceso,
        FL_ID_TIPO_MOV=fl_id_tipo_mov,
        TABLA_NCI_VIV=conf.p_tabla_nci_viv,
        SIEFORE=conf.p_siefore,
        CX_CRE_USUARIO=conf.cx_cre_usuario,
        CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    )
else:
    statement_006b = query.get_statement(
        "NB_PATRIF_MOV_TRANS_INFO_02_006B_TRANSFORM_DATA_348_349.sql",
        SR_FOLIO=params.sr_folio,
        SR_SUBPROCESO=params.sr_subproceso,
        FL_ID_TIPO_MOV=fl_id_tipo_mov,
        TABLA_NCI_VIV=conf.p_tabla_nci_viv,
        SIEFORE=conf.p_siefore,
        CX_CRE_USUARIO=conf.cx_cre_usuario,
        CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    )
if conf.debug:
    display(db.sql_delta(statement_006b))

# COMMAND ----------

# DBTITLE 1,Insertar datos transformados (INSERT en OCI)
logger.info("Insertando datos transformados en OCI")
table_name_target = "CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS"
db.write_data(db.sql_delta(statement_006b), table_name_target, "default", "append")

logger.info(
    f"Proceso completado para folio: {params.sr_folio}, subproceso: {params.sr_subproceso}"
)

# COMMAND ----------

# DBTITLE 1,Envio de notificacion de terminacion exitosa
Notify.send_notification("PATRIF_0800_GMO", params)

# COMMAND ----------

# DBTITLE 1,Limpieza de memoria
CleanUpManager.cleanup_notebook(locals())
