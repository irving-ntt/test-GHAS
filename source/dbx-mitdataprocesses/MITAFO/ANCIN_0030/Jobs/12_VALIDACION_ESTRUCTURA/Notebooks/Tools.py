# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

# Crear la instancia con los parámetros esperados
params = WidgetParams({    
    "sr_folio" : str ,
    "sr_proceso" : str ,
    "sr_subproceso" : str ,
    "sr_subetapa" : str 
})

# Validar widgets
params.validate()

# 🚀 **Ejemplo de Uso**
conf = ConfManager()
db = DBXConnectionManager()
query = QueryManager()

# COMMAND ----------

statement_001 = query.get_statement(
    "prueba.sql",
    
    
)

# COMMAND ----------


df = db.read_data("prueba", statement_001)
display(df)

# COMMAND ----------

dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/PROCESS/DBX/')


# COMMAND ----------

dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT070_202208241640309239_3.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/PPA_RCAT076_202208241640309239_3.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT070_202411291405334352_3.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK/PPA_RCAT070_202411291405334352_3.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT070_202499999994568484_3.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/ArchivosOK/PPA_RCAT070_202499999994568484_3.DAT')

# COMMAND ----------

dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/20251001_182750_053_PREFT.DP.P01534.F251001.DISPAC.C808.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/20251001_182750_053_PREFT.DP.P01534.F251001.DISPAC.C808.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250905.DISPCG.C740.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250905.DISPCG.C740.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250922.DISPCG.C016_B.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250922.DISPCG.C016_B.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250306.DISPCG.C005.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C005.DAT')


# COMMAND ----------

SETTINGS.GENERAL.EXTERNAL_LOCATION + conf.err_repo_path + "/" + params.sr_id_archivo + "_" + conf.output_file_name_001

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250304.DISPAF.C403.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250304.DISPAF.C403.DAT')


# COMMAND ----------

# MAGIC %md
# MAGIC COPIADO A VOLUMEN

# COMMAND ----------

#print(full_file_name)
#dbutils.fs.ls(full_file_name)
#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/INF/65675_Carga_Archivo.csv')

dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/PROCESS/DBX/PPA_RCAT069_201906251955040001_3.DAT','/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/')

# COMMAND ----------

dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/PROCESS/DBX/')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT069_201906251955040001_3.DAT','abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/')

# COMMAND ----------

# MAGIC %md
# MAGIC BORRADO DE ARCHIVOS

# COMMAND ----------

dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/INF/PREFT.DP.P01534.F250210.DISPAC.C005.DAT')

# COMMAND ----------

# MAGIC %md
# MAGIC DOIMSS
# MAGIC 8

# COMMAND ----------

PREFT.DP.P01534.F250304.DISPAF.C400.DAT
PREFT.DP.P01534.F250304.DISPAF.C401.DAT
PREFT.DP.P01534.F250304.DISPAF.C402.DAT
PREFT.DP.P01534.F250304.DISPAF.C403.DAT

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250304.DISPAF.C450.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250304.DISPAF.C450.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250304.DISPAF.C401.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250304.DISPAF.C401.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250304.DISPAF.C402.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250304.DISPAF.C402.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250304.DISPAF.C403.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250304.DISPAF.C403.DAT')

# COMMAND ----------

# MAGIC %md
# MAGIC GUBERNAMENTAL
# MAGIC 439

# COMMAND ----------

RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C001
RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C002
RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C003
RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C004
RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C005

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250306.DISPCG.C001_PRUEBA_4.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C001_PRUEBA_4.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250306.DISPCG.C002.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C002.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250306.DISPCG.C003.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C003.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250306.DISPCG.C004.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C004.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250306.DISPCG.C005.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250306.DISPCG.C005.DAT')


# COMMAND ----------


dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250414.DISPCG.C165.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250414.DISPCG.C165.DAT')

# COMMAND ----------

# MAGIC %md
# MAGIC ITIMSS 121
# MAGIC

# COMMAND ----------

PREFT.DP.P01534.F250305.LOTEIN.C002
PREFT.DP.P01534.F250305.LOTEIN.C003
PREFT.DP.P01534.F250305.LOTEIN.C004
PREFT.DP.P01534.F250305.LOTEIN.C005
PREFT.DP.P01534.F250305.LOTEIN.C006

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250305.LOTEIN.C002.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250305.LOTEIN.C002.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250305.LOTEIN.C003.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250305.LOTEIN.C003.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250305.LOTEIN.C004.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250305.LOTEIN.C004.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250305.LOTEIN.C005.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250305.LOTEIN.C005.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250305.LOTEIN.C006.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250305.LOTEIN.C006.DAT')


# COMMAND ----------

# MAGIC %md
# MAGIC ACLARACIONES ESPECIALES
# MAGIC 118

# COMMAND ----------

PREFT.DP.P01534.F250210.DISPAC.C002
PREFT.DP.P01534.F250210.DISPAC.C003
PREFT.DP.P01534.F250210.DISPAC.C004
PREFT.DP.P01534.F250210.DISPAC.C005

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250210.DISPAC.C002.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250210.DISPAC.C002')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250210.DISPAC.C003.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250210.DISPAC.C003.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250210.DISPAC.C004.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250210.DISPAC.C004.DAT')



# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250210.DISPAC.C005.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250210.DISPAC.C005.DAT')


# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F250305.LOTEIN.C006.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F250305.LOTEIN.C006.DAT')


# COMMAND ----------

dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/PPA_RCAT064_202501091933016835_2.DAT','/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT064_202501091933016835_2.DAT')

# COMMAND ----------

dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/INTEGRITY/202501091933016835_Matriz_Convivencia.CSV')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PREFT.DP.P01534.F210928.LOTEIN.C6012.dat', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PREFT.DP.P01534.F210928.LOTEIN.C6012.DAT')


# COMMAND ----------

# MAGIC %md
# MAGIC DOISSTE 120

# COMMAND ----------

MIKE_PROD_PIRFT.DP.P01534.F250214.DISPRET.C957
YAQUI_PROD_PIRFT.DP.P01534.F250103.DISPRET.C357
YAQUI_PROD_PIRFT.DP.P01534.F250108.DISPRET.C427
YAQUI_PROD_PIRFT.DP.P01534.F250113.DISPRET.C438
YAQUI_PROD_PIRFT.DP.P01534.F250117.DISPRET.C517
YAQUI_PROD_PIRFT.DP.P01534.F250124.DISPRET.C599
YAQUI_PROD_PIRFT.DP.P01534.F250129.DISPRET.C697

# COMMAND ----------

20250805_125017_621_PIRFT.DP.P01534.F250804.DISPRET.C559
20250806_132617_224_PIRFT.DP.P01534.F250806.DISPRET.C302

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/20250806_132617_224_PIRFT.DP.P01534.F250806.DISPRET.C302.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/20250806_132617_224_PIRFT.DP.P01534.F250806.DISPRET.C302.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/20250805_125017_621_PIRFT.DP.P01534.F250804.DISPRET.C559.dat', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/20250805_125017_621_PIRFT.DP.P01534.F250804.DISPRET.C559.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/MIKE_PROD_PIRFT.DP.P01534.F250214.DISPRET.C957.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/MIKE_PROD_PIRFT.DP.P01534.F250214.DISPRET.C957.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/YAQUI_PROD_PIRFT.DP.P01534.F250103.DISPRET.C357.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/YAQUI_PROD_PIRFT.DP.P01534.F250103.DISPRET.C357.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/YAQUI_PROD_PIRFT.DP.P01534.F250108.DISPRET.C427.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/YAQUI_PROD_PIRFT.DP.P01534.F250108.DISPRET.C427.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/YAQUI_PROD_PIRFT.DP.P01534.F250113.DISPRET.C438.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/YAQUI_PROD_PIRFT.DP.P01534.F250113.DISPRET.C438.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/YAQUI_PROD_PIRFT.DP.P01534.F250117.DISPRET.C517.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/YAQUI_PROD_PIRFT.DP.P01534.F250117.DISPRET.C517.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/YAQUI_PROD_PIRFT.DP.P01534.F250124.DISPRET.C599.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/YAQUI_PROD_PIRFT.DP.P01534.F250124.DISPRET.C599.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/YAQUI_PROD_PIRFT.DP.P01534.F250129.DISPRET.C697.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/YAQUI_PROD_PIRFT.DP.P01534.F250129.DISPRET.C697.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/YAQUI_PROD_PIRFT.DP.P01534.F250129.DISPRET.C697_3.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/YAQUI_PROD_PIRFT.DP.P01534.F250129.DISPRET.C697_3.DAT')

# COMMAND ----------

# MAGIC %md
# MAGIC DGISSSTE 210

# COMMAND ----------

MIKE_PROD_PIRFT.DP.P01534.F250116.DISPSCS.C050
MIKE_PROD_PIRFT.DP.P01534.F250217.DISPSCS.C050
MIKE_PROD_PIRFT.DP.P01534.F250314.DISPSCS.C050

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PIRFT.DP.P01534.F251208.DISPSCS.C110.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PIRFT.DP.P01534.F251208.DISPSCS.C110.DAT')

# COMMAND ----------

dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/MIKE_PROD_PIRFT.DP.P01534.F250116.DISPSCS.C050_2.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/MIKE_PROD_PIRFT.DP.P01534.F250116.DISPSCS.C050_2.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/MIKE_PROD_PIRFT.DP.P01534.F250217.DISPSCS.C050.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/MIKE_PROD_PIRFT.DP.P01534.F250217.DISPSCS.C050.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/MIKE_PROD_PIRFT.DP.P01534.F250217.DISPSCS.C050_2.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/MIKE_PROD_PIRFT.DP.P01534.F250217.DISPSCS.C050_2.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/MIKE_PROD_PIRFT.DP.P01534.F250314.DISPSCS.C050.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/MIKE_PROD_PIRFT.DP.P01534.F250314.DISPSCS.C050.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PIRFT.DP.P01534.F250801.DISPSCS.C027.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PIRFT.DP.P01534.F250801.DISPSCS.C027.DAT')

# COMMAND ----------

# MAGIC %md
# MAGIC ITISSSTE 122

# COMMAND ----------

MIKE_PROD_PIRFT.DP.P01534.F250124.DISPTRANS.C578
MIKE_PROD_PIRFT.DP.P01534.F250221.DISPTRANS.C110
YAQUI_PROD_PIRFT.DP.P01534.F250124.DISPTRANS.C578

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/MIKE_PROD_PIRFT.DP.P01534.F250124.DISPTRANS.C578.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/MIKE_PROD_PIRFT.DP.P01534.F250124.DISPTRANS.C578.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/MIKE_PROD_PIRFT.DP.P01534.F250221.DISPTRANS.C110.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/MIKE_PROD_PIRFT.DP.P01534.F250221.DISPTRANS.C110.DAT')

# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/YAQUI_PROD_PIRFT.DP.P01534.F250124.DISPTRANS.C578.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/YAQUI_PROD_PIRFT.DP.P01534.F250124.DISPTRANS.C578.DAT')

# COMMAND ----------


dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/MIKE_PROD_PIRFT.DP.P01534.F250124.DISPTRANS.C578_2.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/MIKE_PROD_PIRFT.DP.P01534.F250124.DISPTRANS.C578_2.DAT')



# COMMAND ----------

dbutils.fs.cp('/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCAT069_202509041636563019_3.DAT', 'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/IN/PPA_RCAT069_202509041636563019_3.DAT')

# COMMAND ----------

# MAGIC %md
# MAGIC TRASPASOS
