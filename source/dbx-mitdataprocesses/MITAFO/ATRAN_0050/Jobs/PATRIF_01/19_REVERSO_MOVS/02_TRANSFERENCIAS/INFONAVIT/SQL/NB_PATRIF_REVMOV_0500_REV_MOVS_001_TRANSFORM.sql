-- NB_PATRIF_REVMOV_0500_REV_MOVS_001_TRANSFORM.sql
-- ========================================================================
-- Propósito: Transformar registros para actualizar estatus en Pre-Movimientos
-- ========================================================================
-- Stage Original: TR_300_ACT_PRE_MOV (Transformer)
-- Derivaciones:
--   FTC_FOLIO = FTC_FOLIO_BITACORA
--   FTN_NUM_CTA_INVDUAL = FTN_NUM_CTA_AFORE
--   FTN_ID_MARCA = FTN_ID_MARCA
--   FCN_ID_SUBPROCESO = p_SR_SUBPROCESO
--   FCD_FEH_ACT = CurrentTimestamp()
--   FCC_USU_ACT = sr_usuario
--   FTN_PRE_MOV_GENERADO = '3' (constante - estatus rechazado)
-- ========================================================================

SELECT 
    FTC_FOLIO_BITACORA AS FTC_FOLIO,
    FTN_NUM_CTA_AFORE AS FTN_NUM_CTA_INVDUAL,
    FTN_ID_MARCA,
    #SR_SUBPROCESO# AS FCN_ID_SUBPROCESO,
    FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Mexico_City') AS FCD_FEH_ACT,
    '#SR_USUARIO#' AS FCC_USU_ACT,
    3 AS FTN_PRE_MOV_GENERADO
FROM #CATALOG_SCHEMA#.RESULTADO_RECH_PROCESAR_#SR_FOLIO#
WHERE FTC_FOLIO_BITACORA IS NOT NULL
  AND FTN_NUM_CTA_AFORE IS NOT NULL
  AND FTN_ID_MARCA IS NOT NULL

