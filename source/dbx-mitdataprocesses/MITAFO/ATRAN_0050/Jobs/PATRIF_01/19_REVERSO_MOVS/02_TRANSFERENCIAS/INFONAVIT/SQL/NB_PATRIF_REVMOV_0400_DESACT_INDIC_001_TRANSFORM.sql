-- NB_PATRIF_REVMOV_0400_DESACT_INDIC_001_TRANSFORM.sql
-- ========================================================================
-- Propósito: Transformar registros para desactivar indicador de crédito vivienda
-- ========================================================================
-- Stage Original: TR_300_DESACT_INDI (Transformer)
-- Derivaciones:
--   FCN_ID_IND_CTA_INDV = LK_DS200_TO_TR300.FCN_ID_IND_CTA_INDV
--   FTN_NUM_CTA_INVDUAL = LK_DS200_TO_TR300.FTN_NUM_CTA_AFORE
--   FFN_ID_CONFIG_INDI = 14 (constante)
--   FCC_VALOR_IND = IF p_SR_SUBPROCESO=368 THEN '1' ELSE IF p_SR_SUBPROCESO=364 THEN '2' ELSE '3'
--   FTC_VIGENCIA = '0' (constante - desactivar)
--   FTD_FEH_ACT = CurrentTimestamp() (zona horaria México)
--   FCC_USU_ACT = sr_usuario
-- ========================================================================

SELECT 
    FCN_ID_IND_CTA_INDV,
    FTN_NUM_CTA_AFORE AS FTN_NUM_CTA_INVDUAL,
    14 AS FFN_ID_CONFIG_INDI,
    CASE 
        WHEN #SR_SUBPROCESO# = 368 THEN '1'
        WHEN #SR_SUBPROCESO# = 364 THEN '2'
        ELSE '3'
    END AS FCC_VALOR_IND,
    '0' AS FTC_VIGENCIA,
    FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Mexico_City') AS FTD_FEH_ACT,
    '#SR_USUARIO#' AS FCC_USU_ACT
FROM #CATALOG_SCHEMA#.RESULTADO_RECH_PROCESAR_#SR_FOLIO#
WHERE FCN_ID_IND_CTA_INDV IS NOT NULL

