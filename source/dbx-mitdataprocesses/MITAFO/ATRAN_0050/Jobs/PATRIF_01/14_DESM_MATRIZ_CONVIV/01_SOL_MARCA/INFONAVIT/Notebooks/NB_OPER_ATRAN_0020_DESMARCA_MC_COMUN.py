# Databricks notebook source
'''
Descripcion:
    Desmarca Matriz de Convivencia
Subetapa:
    Desmarca Matriz de Convivencia
Tramite:
    
Tablas Input:
    TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01{params.sr_folio}
Tablas Output:
    TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
Tablas DELTA:
    DELTA_DESMARCA_{params.sr_folio}

Archivos SQL:
    0010_DESMARCA.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_path_arch": str,
    "sr_conv_ingty": str,
    "sr_tipo_ejecucion":str,
    #valores obligatorios
    "sr_etapa": str,
    "sr_instancia_proceso": str,    
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# Validar widgets
params.validate()

# 🚀 **Ejemplo de Uso**
conf = ConfManager()
db = DBXConnectionManager()
query = QueryManager()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generacion para archivo 69 ###

# COMMAND ----------

# DBTITLE 1,DELTA CON LOS REGISTROS CON MARCA 1
statement_marca_conv = query.get_statement(
    "COMUN_0010_DESMARCA.sql",
    SR_FOLIO=params.sr_folio    
 )

execution = db.execute_oci_dml(
   statement=statement_marca_conv, async_mode=False
)

df = db.read_data("default", statement_marca_conv)


db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}", df, "overwrite")



if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01_{params.sr_folio}"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### DESMARCA CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA ###

# COMMAND ----------

# Extrae unicamente DATOS para el update a Matriz de Convivencia en Datastage es solo por el ID, verificar
# Datos almacenados en TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_01 
statement_desmc_matriz_conv = query.get_statement(
    "COMUN_020_0100_INFO_PARA_DESMARCA_MATRIZ.sql",
    SR_FOLIO=params.sr_folio    
 )

execution = db.execute_oci_dml(
   statement=statement_desmc_matriz_conv, async_mode=False
)

df = db.read_data("default", statement_desmc_matriz_conv)


db.write_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}", df, "overwrite")



if conf.debug:
    display(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}"))


# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN MATRIZ_AUX
statement = query.get_statement(
    "COMUN_MCV_130_TD_06_DEL_MATRIZ_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

#INSERTA EN MATRIZ_AUX
table_name = "CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX"

db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# REALIZA MERGE/UPDATE 
statement = query.get_statement(
    "ATRAN_DESM_020_0200_MERGE_UPD_MATRIZ_CONVIVENCIA.sql",
    SR_FOLIO = params.sr_folio,
    SR_USUARIO = params.sr_usuario,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN MATRIZ_AUX PARA DEJAR LA TABLA VACIA
statement = query.get_statement(
    "COMUN_MCV_130_TD_06_DEL_MATRIZ_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DESMARCA CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV
# MAGIC ### ## 

# COMMAND ----------

# Extrae unicamente DATOS para el update a VAL MATRIZ  en Datastage es solo por el Folio

#  statement_desmc_val_matriz_conv = query.get_statement(
#     "COMUN_0020_0100_INFO_PARA_DESMARCA_VAL_MATRIZ.sql",
#     SR_FOLIO=params.sr_folio    
#  )

# execution = db.execute_oci_dml(
#    statement=statement_desmc_val_matriz_conv, async_mode=False
# )

# df = db.read_data("default", statement_desmc_val_matriz_conv)


# db.write_delta(f"TEMP_DELTA_COMUN_VAL_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}", df, "overwrite")



# if conf.debug:
#     display(db.read_delta(f"TEMP_DELTA_COMUN_VAL_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}"))


# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN VAL_MATRIZ_AUX
# statement = query.get_statement(
#     "COMUN_MCV_130_TD_06_DEL_VAL_MATRIZ_AUX.sql",
#     SR_FOLIO=params.sr_folio,
#     hints="/*+ PARALLEL(8) */",
# )
# execution = db.execute_oci_dml(
#     statement=statement, async_mode=False
# )

# COMMAND ----------

#INSERTA EN VAL MATRIZ_AUX
# table_name = "CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX"

# db.write_data(db.read_delta(f"TEMP_DELTA_COMUN_VAL_MCV_DELTA_INFO_DESMARCA_02_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

# REALIZA MERGE/UPDATE 
# statement = query.get_statement(
#     "COMUN_ATRAN_DESM_020_0200_MERGE_UPD_VAL_MATRIZ.sql",
#     SR_FOLIO = params.sr_folio,
#     SR_USUARIO = params.sr_usuario,
#     hints="/*+ PARALLEL(8) */",
# )
# execution = db.execute_oci_dml(
#     statement=statement, async_mode=False
# )

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO EN VAL_MATRIZ_AUX
# statement = query.get_statement(
#     "COMUN_MCV_130_TD_06_DEL_VAL_MATRIZ_AUX.sql",
#     SR_FOLIO=params.sr_folio,
#     hints="/*+ PARALLEL(8) */",
# )
# execution = db.execute_oci_dml(
#     statement=statement, async_mode=False
# )
