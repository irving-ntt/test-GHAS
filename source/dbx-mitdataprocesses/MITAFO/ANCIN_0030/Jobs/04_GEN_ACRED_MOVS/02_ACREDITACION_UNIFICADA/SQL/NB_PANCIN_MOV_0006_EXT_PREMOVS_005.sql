-- ==========================================
-- QUERY: NB_PANCIN_MOV_0006_EXT_PREMOVS_005.sql
-- DESCRIPCIÓN: Combinar ambos flujos y generar resultado final
--              (FU_800 en DataStage)
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CATALOG_SCHEMA: Catálogo y esquema (ej: catalog.schema)
--   @SR_FOLIO: Folio del proceso
--   @CLV_UNID_NEG: Unidad de negocio de CRE
--   @CLV_SUFIJO_06: Sufijo 06
--   @EXT_ARC_DS: Extensión de archivos dataset
-- ==========================================
-- TIPO: DELTA
-- USO: db.sql_delta()
-- ==========================================

-- Combinar ambos flujos usando UNION ALL (equivalente a FU_800 Funnel)
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
    FTN_ID_MARCA,
    FTB_ESTATUS_MARCA
FROM #CATALOG_SCHEMA#.TEMP_FLUJO_1_#SR_FOLIO#

UNION ALL

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
    FTN_ID_MARCA,
    FTB_ESTATUS_MARCA
FROM #CATALOG_SCHEMA#.TEMP_FLUJO_2_#SR_FOLIO#

ORDER BY FTN_NUM_CTA_INVDUAL, FCN_ID_CAT_SUBCTA 