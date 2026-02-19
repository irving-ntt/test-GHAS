-- NB_PATRIF_REVMOV_0300_ARCH_INGTY_003_SUMARIO.sql
-- ========================================================================
-- Propósito: Eliminar duplicados del sumario
-- ========================================================================
-- Stage Original: RD_802_REG_UNIC (Remove Duplicates)
-- Operator: remdup
-- Keep: first
-- Key: ID_REGISTRO
-- ========================================================================

SELECT 
    ID_REGISTRO,
    DETALLE
FROM (
    SELECT 
        ID_REGISTRO,
        DETALLE,
        ROW_NUMBER() OVER (PARTITION BY ID_REGISTRO ORDER BY (SELECT NULL)) AS rn
    FROM #CATALOG_SCHEMA#.RESULTADO_SUMARIO_INTEGRITY_#SR_FOLIO#
    WHERE ID_REGISTRO IS NOT NULL
)
WHERE rn = 1

