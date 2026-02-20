-- ==========================================
-- QUERY: NB_PANCIN_MOV_0016_SUF_SALDO_003.sql
-- DESCRIPCIÓN: Filtrar datos con marca (FT_400 en DataStage)
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CATALOG_SCHEMA: Catálogo y esquema (ej: catalog.schema)
--   @SR_FOLIO: Folio del proceso
-- ==========================================
-- TIPO: DELTA
-- USO: db.sql_delta()
-- ==========================================

SELECT
    FTN_ID_MARCA,
    FTC_FOLIO AS VFOLIO,
    FTN_NUM_CTA_INVDUAL,
    FRN_ID_MOV_SUBCTA,
    FTB_ESTATUS_MARCA
FROM #CATALOG_SCHEMA#.RESULTADO_MATRIZ_CONVIVENCIA_#SR_FOLIO#
WHERE FTB_ESTATUS_MARCA = '1'