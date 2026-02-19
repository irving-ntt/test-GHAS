-- ==========================================
-- QUERY: NB_PANCIN_MOV_0010_EXT_PREMOVS_001.sql
-- DESCRIPCIÓN: Filtrar datos con marca (FI_110 en DataStage)
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
    FTN_ID_MARCA,
    FCN_ID_SALDO_OPERA
FROM #CATALOG_SCHEMA#.RESULTADO_PREMOVIMIENTOS_#SR_FOLIO#
WHERE FTB_ESTATUS_MARCA = 1 
   OR ((FCN_ID_SUBPROCESO = 130 OR FCN_ID_SUBPROCESO = 3281 
        OR FCN_ID_SUBPROCESO = 6398 OR FCN_ID_SUBPROCESO = 10555) 
       AND FCN_ID_TIPO_MOV = 181) 