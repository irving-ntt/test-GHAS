-- ==========================================
-- QUERY: NB_PANCIN_MOV_0016_SUF_SALDO_004.sql
-- DESCRIPCIÓN: Realizar join con datos filtrados (JO_300 en DataStage)
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
    t.FTC_FOLIO,
    t.FTN_NUM_CTA_INVDUAL,
    t.FCN_ID_TIPO_SUBCTA,
    t.FCN_ID_SIEFORE,
    t.FTN_DEDUCIBLE,
    t.FCN_ID_PLAZO,
    t.FTF_MONTO_PESOS,
    t.FTF_MONTO_ACCIONES,
    t.FTF_MONTO_PESOS_SOL,
    t.FTF_MONTO_ACCIONES_SOL
FROM #CATALOG_SCHEMA#.TEMP_TRANSFORMACION_#SR_FOLIO# t
INNER JOIN #CATALOG_SCHEMA#.TEMP_FILTRADO_MARCA_#SR_FOLIO# f
    ON t.VFOLIO = f.VFOLIO
    AND t.FTN_NUM_CTA_INVDUAL = f.FTN_NUM_CTA_INVDUAL
    AND t.FRN_ID_MOV_SUBCTA = f.FRN_ID_MOV_SUBCTA