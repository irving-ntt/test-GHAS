-- ==========================================
-- QUERY: NB_PANCIN_MOV_0016_SUF_SALDO_002.sql
-- DESCRIPCIÓN: Procesar transformación de datos (TF_200 en DataStage)
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
    CASE 
        WHEN FOLIO IS NULL THEN FTC_FOLIO 
        ELSE FOLIO 
    END AS VFOLIO,
    FTC_FOLIO,
    FTN_NUM_CTA_INVDUAL,
    FCN_ID_TIPO_SUBCTA,
    FCN_ID_SIEFORE,
    FTN_DEDUCIBLE,
    FCN_ID_PLAZO,
    FRN_ID_MOV_SUBCTA,
    FTF_MONTO_PESOS,
    FTF_MONTO_ACCIONES,
    FTF_MONTO_PESOS_SOL,
    FTF_MONTO_ACCIONES_SOL
FROM #CATALOG_SCHEMA#.TEMP_PREMOVIMIENTOS_#SR_FOLIO#