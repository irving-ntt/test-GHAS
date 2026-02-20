-- =============================================================================
-- NB_PATRIF_GMO_0100_DEV_FOV_001_OCI_DEV_SALDOS.sql
-- =============================================================================
-- Propósito: Extraer datos de devolución de saldos excedentes FOVISSSTE desde Oracle
-- Tipo: OCI (Oracle)
-- Stage Original: BD_100_DEV_SALDOS
-- =============================================================================

SELECT 
    FTN_NUM_CTA_INVDUAL         AS FTN_NUM_CTA_INVDUAL,
    NULL                        AS FTN_IND_SDO_DISP,
    FTN_NUM_APLI_INTE_VIVI08    AS FTN_SDO_AIVS_08,
    FTN_NUM_APLI_INTE_VIVI92    AS FTN_SDO_AIVS_92,
    FTN_SALDO_TOT_VIVI08        AS FTN_MONTO_PESOS_08,
    FTN_SALDO_TOT_VIVI92        AS FTN_MONTO_PESOS_92,
    NULL                        AS FTN_NUM_APLI_INTE_VIV_08,
    NULL                        AS FTN_NUM_APLI_INTE_VIV_92,
    NULL                        AS FTN_SALDO_VIV,
    FTC_ESTATUS                 AS FCN_ESTATUS,
    FTC_FOLIO,
    138                         AS FCN_ID_REGIMEN,
    FCN_ID_SUBPROCESO,
    NULL                        AS FTN_CONTA_SERV,
    FTN_MOTIVO_RECHAZO          AS FTN_MOTI_RECH,
    ADD_MONTHS(TRUNC(SYSDATE,'MONTH'),1) AS FECHA_VALOR_AIVS
FROM #CX_PRO_ESQUEMA#.#TL_PRO_DEV_SLD_ECX_FOV#
WHERE FTC_FOLIO = '#SR_FOLIO#' 
  AND FTC_ESTATUS = '#ESTATUS#'

