-- NB_PATRIF_REVMOV_0300_ARCH_INGTY_002_ENCABEZADO.sql
-- ========================================================================
-- Propósito: Generar registro de encabezado (tipo '01')
-- ========================================================================
-- Stages Originales: CP_804_SEPARA + TR_805_ENCABEZADO + RD_806_REG_UNIC
-- TR_805_ENCABEZADO - Variables de Stage:
--   vFECHA = DateToString(CurrentDate(), "%yyyy%mm%dd")
--   vNUMREG = Right(TrimB(Change(DecimalToString(CONTADOR), ".", "")), 9)
-- Output:
--   ID_REGISTRO = '01'
--   DETALLE = '01' : vFECHA : vNUMREG : Str(' ', 109)
-- RD_806_REG_UNIC - Remove Duplicates:
--   Operator: remdup
--   Keep: first
--   Key: ID_REGISTRO
-- ========================================================================

SELECT 
    ID_REGISTRO,
    DETALLE
FROM (
    SELECT 
        '01' AS ID_REGISTRO,
        CONCAT(
            '01',
            DATE_FORMAT(
                TO_DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Mexico_City')),
                'yyyyMMdd'
            ),
            LPAD(CAST(CONTADOR AS STRING), 9, ' '),
            REPEAT(' ', 109)
        ) AS DETALLE,
        ROW_NUMBER() OVER (PARTITION BY '01' ORDER BY (SELECT NULL)) AS rn
    FROM #CATALOG_SCHEMA#.TEMP_AG_MAX_#SR_FOLIO#
)
WHERE rn = 1

