-- =====================================================================================
-- WF_PATRIF_MOV_TRANS_INFO_02_001_READ_DATASET.sql
-- =====================================================================================
-- Lectura del insumo inicial desde tabla Delta definida en conf.delta_500_saldo
-- =====================================================================================

SELECT 
    FTN_NUM_CTA_INVDUAL,
    FTN_IND_SDO_DISP,
    FTN_SDO_AIVS,
    FTN_VALOR_AIVS,
    FTN_MONTO_PESOS,
    FCN_ID_TIPO_SUBCTA,
    FTN_NUM_APLI_INTE_VIV,
    FTN_SALDO_VIV,
    FCN_ESTATUS,
    FTC_FOLIO,
    FCN_ID_REGIMEN,
    FTN_ID_SUBP,
    FCN_ID_VALOR_ACCION,
    FTN_CONTA_SERV,
    FTN_MOTI_RECH,
    FTD_FEH_CRE,
    FTC_USU_CRE
FROM #CATALOG_SCHEMA#.#DELTA_TABLE#
WHERE FTC_FOLIO = '#SR_FOLIO#' 