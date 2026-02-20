-- ==========================================
-- QUERY: NB_PANCIN_MOV_0006_EXT_PREMOVS_004.sql
-- DESCRIPCIÓN: Procesar flujo 2 - Join entre premovimientos y datos de marca
--              (JO_400 en DataStage)
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

WITH FLUJO_2 AS (
    SELECT 
        FCN_ID_CAT_SUBCTA,
        FTC_FOLIO,
        FTC_FOLIO_REL,
        FTF_MONTO_PESOS,
        FTF_MONTO_ACCIONES,
        FCD_FEH_ACCION,
        FTN_NUM_CTA_INVDUAL,
        FCN_ID_TIPO_MOV,
        FCC_TABLA_NCI_MOV,
        FNN_ID_REFERENCIA,
        FCN_ID_SUBPROCESO,
        FTF_MONTO_PESOS_SOL,
        FTF_MONTO_ACCIONES_SOL,
        FCN_ID_CONCEPTO_MOV,
        FCN_ID_SIEFORE,
        FCN_ID_SALDO_OPERA,
        FRN_ID_MOV_SUBCTA,
        FOLIO
    FROM #CATALOG_SCHEMA#.TEMP_PREMOVIMIENTOS_#SR_FOLIO#
    WHERE FCN_ID_TIPO_MOV = 181 
      AND FCN_ID_SUBPROCESO NOT IN (130, 6398, 3281, 10555)
)
SELECT 
    f.FCN_ID_CAT_SUBCTA,
    f.FTC_FOLIO,
    f.FTC_FOLIO_REL,
    f.FTF_MONTO_PESOS,
    f.FTF_MONTO_ACCIONES,
    f.FCD_FEH_ACCION,
    f.FTN_NUM_CTA_INVDUAL,
    f.FCN_ID_TIPO_MOV,
    f.FCC_TABLA_NCI_MOV,
    f.FNN_ID_REFERENCIA,
    f.FCN_ID_SUBPROCESO,
    f.FTF_MONTO_PESOS_SOL,
    f.FTF_MONTO_ACCIONES_SOL,
    f.FCN_ID_CONCEPTO_MOV,
    f.FCN_ID_SIEFORE,
    f.FCN_ID_SALDO_OPERA,
    f.FRN_ID_MOV_SUBCTA,
    m.FTN_ID_MARCA,
    m.FTB_ESTATUS_MARCA
FROM FLUJO_2 f
LEFT JOIN #CATALOG_SCHEMA#.RESULTADO_MATRIZ_CONVIVENCIA_#SR_FOLIO# m
    ON f.FTC_FOLIO = m.FTC_FOLIO 
    AND f.FTN_NUM_CTA_INVDUAL = m.FTN_NUM_CTA_INVDUAL 