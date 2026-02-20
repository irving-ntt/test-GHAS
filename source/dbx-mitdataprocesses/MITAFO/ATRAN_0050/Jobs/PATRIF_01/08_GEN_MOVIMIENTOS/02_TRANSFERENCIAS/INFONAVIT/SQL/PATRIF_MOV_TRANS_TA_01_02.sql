-- =============================================================================
-- QUERY OCI: Lookup a TTCRXGRAL_TRANS_INFONA (Oracle/OCI)
-- (Resultado: DELTA_TMP_MOV_TRANS_CON_INTERESES)
-- Este query debe ejecutarse en Oracle/OCI y el resultado debe cargarse a Delta
-- =============================================================================

SELECT
 FTC_FOLIO_BITACORA,
 FTN_NUM_CTA_AFORE AS FTN_NUM_CTA_INVDUAL,
 FTN_CONTA_SERV,
 FTN_INTER_SALDO_VIVI97_ULT,
 FTN_INTER_SALDO_VIVI92
FROM #CX_PRO_ESQUEMA#.#p_TL_PRO_TRANS_INFONA#
WHERE FTC_FOLIO_BITACORA = #p_SR_FOLIO#
  AND FTN_ID_SUBP = #p_SR_SUBPROCESO#
  AND FTC_TIPO_ARCH = #p_TIPO_ARCHIVO#