-- =============================================================================
-- NB_PATRIF_GMO_0100_DEV_SALD_EXC_001_OCI_DEV_SALDOS.sql
-- =============================================================================
-- Propósito: Extraer datos de devolución de saldos excedentes desde Oracle
-- Tipo: OCI (Oracle)
-- Stage Original: BD_100_DEV_SALDOS
-- =============================================================================

SELECT 
    FTN_CTA_AFORE         AS FTN_NUM_CTA_INVDUAL,
    NULL                  AS FTN_IND_SDO_DISP,
    FTN_AIVS_VIV_97       AS FTN_SDO_AIVS_97,
    FTN_AIVS_VIV_92       AS FTN_SDO_AIVS_92,
    FTN_SALDO_97          AS FTN_MONTO_PESOS_97,
    FTN_SALDO_92          AS FTN_MONTO_PESOS_92,
    FTN_INTERES_SALDO_97  AS FTN_NUM_APLI_INTE_VIV_97,
    FTN_INTERES_SALDO_92  AS FTN_NUM_APLI_INTE_VIV_92,
    NULL                  AS FTN_SALDO_VIV,
    FTC_ESTATUS           AS FCN_ESTATUS,
    FTC_FOLIO,
    138                   AS FCN_ID_REGIMEN,
    FCN_ID_SUBPROCESO,
    FTN_CONTA_SERV,
    FCN_ID_MOTIVO_RECHAZO AS FTN_MOTI_RECH,
    ADD_MONTHS(TRUNC(SYSDATE,'MONTH'),1) AS FECHA_VALOR_AIVS
FROM #CX_PRO_ESQUEMA#.#TL_CRE_DEVO_SALD_EXC#
WHERE FTC_FOLIO = '#SR_FOLIO#' 
  AND FTC_ESTATUS = '#ESTATUS#'

