-- NB_PATRIF_REVMOV_0200_DESM_NCI_004_DETALLE.sql
-- Propósito: Generar registros de detalle para archivo Integrity (sin duplicados)
-- Tipo: Delta (Databricks)
-- NOTA: Replica Sort: -key FTN_NUM_CTA_AFORE -asc -nulls first + RemDup keep first

SELECT 
    FTN_NUM_CTA_AFORE,
    '02' AS ID_REGISTRO,
    CONCAT(
        '02',
        vCTA,
        vPROCESO,
        vSUBPROCESO,
        vNSS,
        vCURP,
        LPAD('', 79, ' ')
    ) AS DETALLE,
    FTC_FOLIO_BITACORA,
    CAST(CONTADOR AS DECIMAL(9,0)) AS CONTADOR
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY FTN_NUM_CTA_AFORE 
               ORDER BY CONTADOR ASC NULLS FIRST
           ) AS rn
    FROM #CATALOG_SCHEMA#.TEMP_TR500_CAMPOS_#SR_FOLIO#
)
WHERE rn = 1

