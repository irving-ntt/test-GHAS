-- ==========================================
-- QUERY: NB_PANCIN_MOV_0010_EXT_PREMOVS_004.sql
-- DESCRIPCIÓN: Join con movimientos de subcuenta (JO_140 en DataStage)
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
    p.FCN_ID_CAT_SUBCTA,
    p.FTC_FOLIO,
    p.FTC_FOLIO_REL,
    p.FTF_MONTO_PESOS,
    p.FTF_MONTO_ACCIONES,
    p.FCD_FEH_ACCION,
    p.FTN_NUM_CTA_INVDUAL,
    p.FCN_ID_TIPO_MOV,
    p.FCC_TABLA_NCI_MOV,
    p.FNN_ID_REFERENCIA,
    p.FCN_ID_SUBPROCESO,
    p.FTF_MONTO_PESOS_SOL,
    p.FTF_MONTO_ACCIONES_SOL,
    p.FCN_ID_CONCEPTO_MOV,
    p.FCN_ID_SIEFORE,
    p.FTN_ID_MARCA,
    p.FCN_ID_SALDO_OPERA,
    p.FCN_ID_TIPO_SUBCTA,
    p.FCN_ID_GRUPO,
    p.FCN_ID_REGIMEN,
    p.FCN_ID_PLAZO,
    m.FRN_ID_MOV_SUBCTA
FROM #CATALOG_SCHEMA#.TEMP_JOIN_TIPO_SUBCTA_#SR_FOLIO# p
LEFT JOIN #CATALOG_SCHEMA#.TEMP_MOV_SUBCTA_#SR_FOLIO# m
    ON p.FCN_ID_SUBPROCESO = m.FCN_ID_SUBPROCESO
    AND p.FCN_ID_TIPO_SUBCTA = m.FCN_ID_TIPO_SUBCTA
    AND p.FCN_ID_TIPO_MOV = m.FCN_ID_TIPO_MOV 