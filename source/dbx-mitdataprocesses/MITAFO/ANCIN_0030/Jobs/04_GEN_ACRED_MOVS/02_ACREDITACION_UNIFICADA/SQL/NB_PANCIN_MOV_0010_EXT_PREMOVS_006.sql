-- ==========================================
-- QUERY: NB_PANCIN_MOV_0010_EXT_PREMOVS_006.sql
-- DESCRIPCIÓN: Join con valor de acción (JO_160 en DataStage)
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CATALOG_SCHEMA: Catálogo y esquema (ej: catalog.schema)
--   @SR_FOLIO: Folio del proceso
--   @CLV_UNID_NEG: Unidad de negocio de CRE
--   @CLV_SUFIJO_02: Sufijo 02
--   @EXT_ARC_DS: Extensión de archivos dataset
-- ==========================================
-- TIPO: DELTA
-- USO: db.sql_delta()
-- ==========================================

SELECT
    p.FCN_ID_SIEFORE,
    v.FCN_ID_VALOR_ACCION,
    p.FTC_FOLIO,
    p.FTC_FOLIO_REL,
    p.FTF_MONTO_PESOS,
    p.FTF_MONTO_ACCIONES,
    p.FCN_ID_TIPO_MOV,
    p.FCC_TABLA_NCI_MOV,
    p.FNN_ID_REFERENCIA,
    p.FCN_ID_SUBPROCESO,
    p.FTF_MONTO_PESOS_SOL,
    p.FTF_MONTO_ACCIONES_SOL,
    p.FCN_ID_CONCEPTO_MOV,
    p.FTN_ID_MARCA,
    p.FCN_ID_SALDO_OPERA,
    p.FCN_ID_TIPO_SUBCTA,
    p.FCN_ID_PLAZO,
    p.FCN_ID_TIPO_MONTO,
    p.FTN_ORIGEN_APORTACION,
    p.FTN_NUM_CTA_INVDUAL,
    p.FTN_DEDUCIBLE,
    v.FCN_VALOR_ACCION
FROM #CATALOG_SCHEMA#.TEMP_JOIN_CONCEPTO_MOV_#SR_FOLIO# p
INNER JOIN #CATALOG_SCHEMA#.TEMP_VALOR_ACCION_#SR_FOLIO# v
    ON p.FCN_ID_SIEFORE = v.FCN_ID_SIEFORE
    AND p.FCN_ID_REGIMEN = v.FCN_ID_REGIMEN
    AND p.FCD_FEH_ACCION = v.FCD_FEH_ACCION 