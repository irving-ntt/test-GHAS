# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams(
    {
        "sr_folio": str,
        "sr_folio_rel": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_origen_arc": str,
        "sr_fecha_acc": (str, "YYYYMMDD"),
        "sr_fecha_liq": (str, "YYYYMMDD"),
        "sr_tipo_mov": str,
        "sr_reproceso": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_id_snapshot": str,
        "sr_fcc_usu_cre": str,
        "sr_flc_usu_reg": str,
        "cx_cre_esquema": str,
        "tl_cre_dispersion": str,
        "sr_etapa": str,
        "sr_factor": str,
        "cx_crn_esquema": str,
        "cx_pro_esquema": str,
        "tl_pro_aeim": str,
        "tl_crn_tipo_subcta": str,
        "tl_crn_valor_accion": str,
        "tl_crn_config_concep_mov": str,
        "tl_crn_matriz_convivencia": str,
    }
)
params.validate()
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(
    queries_df.filter(
        col("Archivo SQL").startswith("NB_OPER_PANCIN_MOV_0010_EXT_ABONO_AEIM")
    )
)

# COMMAND ----------

statement_001 = query.get_statement(
    "NB_OPER_PANCIN_MOV_0010_EXT_ABONO_AEIM_001.sql",
    SR_FOLIO=params.sr_folio,
    SR_FOLIO_REL=params.sr_folio_rel,
    CX_CRE_ESQUEMA=params.cx_cre_esquema,
    TL_CRE_DISPERSION=params.tl_cre_dispersion,
    CX_CRN_ESQUEMA=params.cx_crn_esquema,
    CX_PRO_ESQUEMA=params.cx_pro_esquema,
    TL_PRO_AEIM=params.tl_pro_aeim,
    TL_CRN_TIPO_SUBCTA=params.tl_crn_tipo_subcta,
    TL_CRN_VALOR_ACCION=params.tl_crn_valor_accion,
    TL_CRN_CONFIG_CONCEP_MOV=params.tl_crn_config_concep_mov,
    TL_CRN_MATRIZ_CONVIVENCIA=params.tl_crn_matriz_convivencia,
    SR_FEC_ACC=params.sr_fecha_acc,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------


temp_view_001 = f"delta_gen_mov_aeim_001_{params.sr_folio}"
db.write_delta(temp_view_001, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view_001))


# COMMAND ----------

statement_002 = query.get_statement(
    "NB_OPER_PANCIN_MOV_0010_EXT_ABONO_AEIM_002.sql",
    SR_FOLIO=params.sr_folio,
    CX_CRE_ESQUEMA=params.cx_cre_esquema,
    TL_CRE_DISPERSION=params.tl_cre_dispersion,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------


temp_view_002 = f"delta_gen_mov_aeim_002_{params.sr_folio}"
db.write_delta(temp_view_002, db.read_data("default", statement_002), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view_002))

# COMMAND ----------

statement_003 = query.get_statement(
    "NB_OPER_PANCIN_MOV_0010_EXT_ABONO_AEIM_003.sql",
    CATALOG_SCHEMA = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    DELTA_001 = temp_view_001,
    DELTA_002 = temp_view_002,
)

# COMMAND ----------

temp_view_003 = f"temp_dispersion_mov_01_{params.sr_folio}"
db.write_delta(temp_view_003, db.sql_delta(statement_003), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view_003))

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
