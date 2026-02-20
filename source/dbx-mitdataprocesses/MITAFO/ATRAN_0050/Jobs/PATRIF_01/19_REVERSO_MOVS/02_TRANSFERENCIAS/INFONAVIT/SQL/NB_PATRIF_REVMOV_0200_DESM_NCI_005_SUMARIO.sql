-- NB_PATRIF_REVMOV_0200_DESM_NCI_005_SUMARIO.sql
-- Propósito: Generar registro de sumario para archivo Integrity
-- Tipo: Delta (Databricks)

SELECT 
    '03' AS ID_REGISTRO,
    CONCAT(
        '03',
        LPAD('', 35, '0'),
        LPAD('', 91, ' ')
    ) AS DETALLE
FROM (
    SELECT 1 AS dummy
) t
LIMIT 1

