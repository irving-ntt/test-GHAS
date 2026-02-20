-- NB_PATRIF_REVMOV_0300_ARCH_INGTY_001_AG_MAX.sql
-- ========================================================================
-- Propósito: Agrupar por FTC_FOLIO_BITACORA y obtener MAX(CONTADOR)
-- ========================================================================
-- Stages Originales: CP_803_SEPARA + AG_804_MAX
-- Operator: group
-- Method: hash
-- Key: FTC_FOLIO_BITACORA
-- Reduce: MAX(CONTADOR)
-- ========================================================================

SELECT 
    FTC_FOLIO_BITACORA,
    MAX(CONTADOR) AS CONTADOR
FROM #CATALOG_SCHEMA#.RESULTADO_DETALLE_INTEGRITY_#SR_FOLIO#
WHERE FTC_FOLIO_BITACORA IS NOT NULL
GROUP BY FTC_FOLIO_BITACORA

