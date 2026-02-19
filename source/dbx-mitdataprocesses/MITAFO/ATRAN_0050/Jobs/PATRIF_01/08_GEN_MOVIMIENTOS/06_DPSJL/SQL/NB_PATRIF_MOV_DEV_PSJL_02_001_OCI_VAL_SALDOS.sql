-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_02_001_OCI_VAL_SALDOS.sql
-- =============================================================================
-- Propósito: Extraer datos de validación de saldos DPSJL con LEFT JOIN
-- Tipo: OCI (Oracle)
-- Stage Original: DB_100_TL_PRO_DEV_PAG_SJL
-- =============================================================================

SELECT 
    DP.FTC_FOLIO,
    DP.FCN_ID_SUBPROCESO,
    DP.FTN_CONSE_REG_LOTE,
    DP.FTN_NUM_CTA_INVDUAL,
    DP.FFN_ID_CONCEPTO_MOV,
    DP.FCN_ID_SIEFORE,
    DP.FTN_SDO_ACCIONES,
    DP.FTN_SDO_PESOS,
    DP.FCN_ID_TIPO_SUBCTA,
    DP.FCN_ID_VALOR_ACCION,
    DP.FTN_VALOR_AIVS
FROM #CX_PRO_ESQUEMA#.#TL_PRO_DEV_PAG_SJL# DEVP 
LEFT JOIN #CX_PRO_ESQUEMA#.#TL_PRO_VAL_SDOS_DPSJL# DP 
    ON DEVP.FTC_FOLIO = DP.FTC_FOLIO 
   AND DEVP.FCN_ID_SUBPROCESO = DP.FCN_ID_SUBPROCESO 
   AND DEVP.FTN_NUM_CTA_INVDUAL = DP.FTN_NUM_CTA_INVDUAL 
   AND DEVP.FTN_CONSE_REG_LOTE = DP.FTN_CONSE_REG_LOTE  
WHERE DEVP.FTC_FOLIO = '#SR_FOLIO#' 
  AND DEVP.FCN_ID_SUBPROCESO = #SR_SUBPROCESO# 
  AND DEVP.FTC_ESTATUS = '1' 
  AND DP.FFN_ID_CONCEPTO_MOV IS NOT NULL

