-- ==========================================
-- AFTER SQL: DB_400_TTSISGRAL_ETL_MOVIMIENTOS
-- ==========================================
-- Actualizar bandera de pre movimiento generado
-- Equivalente al After SQL del stage DB_400_TTSISGRAL_ETL_MOVIMIENTOS
-- ==========================================

DECLARE
  cuenta NUMBER := 0;
BEGIN
  cuenta := 0;
  SELECT COUNT(*)
    INTO cuenta
    FROM #CX_CRE_ESQUEMA#.#TL_CRE_PRE_MOVIMIENTOS#
   WHERE FTC_FOLIO_REL = '#SR_FOLIO#' AND rownum<=1;
  IF (cuenta > 0) THEN
    UPDATE #CX_CRE_ESQUEMA#.#TL_CRE_PRE_MOVIMIENTOS# 
    SET FTN_PRE_MOV_GENERADO = 1, FCD_FEH_ACT = SYSDATE, FCC_USU_ACT = '#CX_CRE_USUARIO#'   
    WHERE FTC_FOLIO_REL = '#SR_FOLIO#';
    COMMIT;
  ELSE
    UPDATE #CX_CRE_ESQUEMA#.#TL_CRE_PRE_MOVIMIENTOS# 
    SET FTN_PRE_MOV_GENERADO = 1, FCD_FEH_ACT = SYSDATE, FCC_USU_ACT = '#CX_CRE_USUARIO#'   
    WHERE FTC_FOLIO = '#SR_FOLIO#';
    COMMIT;
  END IF;
END; 