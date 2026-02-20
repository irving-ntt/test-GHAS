-- NB_PATRIF_REVMOV_0300_ARCH_INGTY_005_UNION_ORDEN.sql
-- ========================================================================
-- Propósito: Unir los tres flujos y ordenar
-- ========================================================================
-- Stages Originales: FN_807_UNE (Funnel) + SR_808_ORDENA (Sort)
-- 
-- FN_807_UNE: 
--   Operator: funnel (UNION ALL)
--   Input 0: RD_802_REG_UNIC (sumario)
--   Input 1: CP_803_SEPARA (detalle)
--   Input 2: RD_806_REG_UNIC (encabezado)
--
-- SR_808_ORDENA:
--   Operator: tsort (total sort)
--   Stable: stable (mantiene orden relativo de registros iguales)
--   Unique: No (permite duplicados, NO elimina)
--   Key: ID_REGISTRO
--   Direction: ASC (orden ascendente)
--   Execmode: seq (sequential - una sola partición)
--   Output: Solo campo DETALLE
-- ========================================================================

SELECT DETALLE
FROM (
    -- Input 0: Sumario
    SELECT 
        ID_REGISTRO,
        DETALLE
    FROM #CATALOG_SCHEMA#.TEMP_SUMARIO_#SR_FOLIO#
    
    UNION ALL
    
    -- Input 1: Detalle (directo de tabla origen, sin intermediario)
    SELECT 
        ID_REGISTRO,
        DETALLE
    FROM #CATALOG_SCHEMA#.RESULTADO_DETALLE_INTEGRITY_#SR_FOLIO#
    WHERE ID_REGISTRO IS NOT NULL
    
    UNION ALL
    
    -- Input 2: Encabezado
    SELECT 
        ID_REGISTRO,
        DETALLE
    FROM #CATALOG_SCHEMA#.TEMP_ENCABEZADO_#SR_FOLIO#
) unified
ORDER BY ID_REGISTRO ASC

