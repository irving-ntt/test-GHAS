-- =============================================================================
-- NB_PATRIF_MOV_DEV_PSJL_01_001_OCI_DEV_PAG_SJL.sql
-- =============================================================================
-- Propósito: Extraer datos de devolución de pagos SJL desde Oracle
-- Tipo: OCI (Oracle)
-- Stage Original: DB_100_TL_PRO_DEV_PAG_SJL
-- =============================================================================

SELECT 
    FTC_FOLIO,
    FCN_ID_SUBPROCESO,
    FTN_CONSE_REG_LOTE,
    FTN_NUM_CTA_INVDUAL,
    NVL(FTN_IMP_RET_DEVOLVER,0)          AS IMP_PESOS_RET,
    NVL(FTN_IMP_ACT_REC_RETIRO_DEV,0)    AS ACT_PESOS_RET,
    NVL(FTN_IMP_ACT_RET_REN_CUENTA,0)    AS REN_PESOS_RET,
    NVL(FTN_IMP_CES_CUO_PATRON_DEV,0)    AS IMP_PESOS_CES_VEJ_PAT,
    NVL(FTN_IMP_CES_CUO_TRABAJ_DEV,0)    AS IMP_PESOS_CES_VEJ_TRA,
    NVL(FTN_IMP_ACT_REC_CUO_PAT_DEV,0)   AS ACT_PESOS_CES_VEJ_PAT,
    NVL(FTN_IMP_ACT_REC_CUO_TRA_DEV,0)   AS ACT_PESOS_CES_VEJ_TRA,
    NVL(FTN_IMP_ACT_REN_CUE_IND_PAT,0)   AS REN_PESOS_CES_VEJ_PAT,
    NVL(FTN_IMP_ACT_REN_CUE_IND_TRA,0)   AS REN_PESOS_CES_VEJ_TRA,
    NVL(FTN_IMP_CUO_SOCIAL_DEV,0)        AS IMP_PESOS_CUO_SOCIAL,
    NVL(FTN_NUM_APLI_VIV_APOR_PAT_DEV,0) AS ACT_AIVS_VIV
FROM #CX_PRO_ESQUEMA#.#TL_PRO_DEV_PAG_SJL# 
WHERE FTC_FOLIO = '#SR_FOLIO#' 
  AND FCN_ID_SUBPROCESO = #SR_SUBPROCESO# 
  AND FTC_ESTATUS = '1'

