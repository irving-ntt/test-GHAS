-- ==========================================
-- QUERY: NB_PANCIN_MOV_0010_EXT_PREMOVS_002.sql
-- DESCRIPCIÓN: Obtener datos sin marca (DS_200_SINMARCA en DataStage)
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CATALOG_SCHEMA: Catálogo y esquema (ej: catalog.schema)
--   @SR_FOLIO: Folio del proceso
--   @CLV_UNID_NEG: Unidad de negocio de CRE
--   @CLV_SUFIJO_01: Sufijo 01
--   @EXT_ARC_DS: Extensión de archivos dataset
-- ==========================================
-- TIPO: DELTA
-- USO: db.sql_delta()
-- ==========================================

SELECT
    FTC_FOLIO,
    FTC_FOLIO_REL,
    FTN_NUM_CTA_INVDUAL,
    FTB_ESTATUS_MARCA
FROM #CATALOG_SCHEMA#.RESULTADO_PREMOVIMIENTOS_#SR_FOLIO# 
WHERE (FCN_ID_TIPO_MOV = 180 AND FTB_ESTATUS_MARCA = '0')
   OR (FCN_ID_TIPO_MOV = 181 AND FTB_ESTATUS_MARCA = '0' 
       AND (FCN_ID_SUBPROCESO <> 130 AND FCN_ID_SUBPROCESO <> 3281 
            AND FCN_ID_SUBPROCESO <> 6398 AND FCN_ID_SUBPROCESO <> 10555)) 