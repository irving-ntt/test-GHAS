-- NB_PATRIF_REVMOV_0100_EXT_TRN_FOV_001_EXTRACT.sql
-- ========================================================================
-- Propósito: Extrae registros rechazados de transferencias FOVISSSTE
-- ========================================================================
-- Stage DataStage: BD_100_TL_PRO_TRANS_FOVISSSTE
-- Output: DS_200_RECH_PROCESAR
-- ========================================================================

SELECT
    M.FTN_NUM_CTA_INVDUAL,
    T.FTC_FOLIO AS FTC_FOLIO_BITACORA,
    T.FTN_NUM_CTA_INVDUAL AS FTN_NUM_CTA_AFORE,
    T.FTC_CURP AS FTC_CURP_TRABA,
    T.FTC_NSS AS FTC_NSS_TRABA_AFORE,
    T.FCN_ID_SUBPROCESO AS SUBPROCESO,
    #SR_PROCESO# AS PROCESO,
    M.FTN_ID_MARCA,
    CAST(NULL AS DECIMAL(38,0)) AS FCN_ID_IND_CTA_INDV
FROM #CX_PRO_ESQUEMA#.#TL_TRANS_FOVISSSTE# T
LEFT JOIN #CX_CRN_ESQUEMA#.#TL_MATRIZ_CONVIV# M 
    ON T.FTC_FOLIO = M.FTC_FOLIO 
    AND T.FTN_NUM_CTA_INVDUAL = M.FTN_NUM_CTA_INVDUAL
WHERE T.FTC_FOLIO = '#SR_FOLIO#'
  AND T.FCN_ID_SUBPROCESO = #SR_SUBPROCESO#
  AND T.FTC_ESTATUS = '0'
  AND T.FTC_TIPO_ARCH = '09'
  AND T.FTN_MOTIVO_RECHAZO = 99

