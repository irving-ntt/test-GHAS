-- Procesar datos identificados: Ordenar + Eliminar duplicados + Aplicar transformaciones
-- Equivalente a SO_410_ORDENA + RM_420_DUPLICADOS + TF_430_MARCA_1 del job DataStage
-- En DataStage: 
-- - SO_410_ORDENA: Ordena por FTN_NSS_CURP, FTN_NUM_CTA_INVDUAL
-- - RM_420_DUPLICADOS: Elimina duplicados (keep first)
-- - TF_430_MARCA_1: Aplica transformaciones (FTC_IDENTIFICADOS=1, FTN_VIGENCIA=LEFT(FCC_VALOR_IND,1))

WITH datos_ordenados AS (
    SELECT 
        FTN_NSS_CURP,
        FTN_NUM_CTA_INVDUAL,
        FTC_NOMBRE_BUC,
        FTC_AP_PATERNO_BUC,
        FTC_AP_MATERNO_BUC,
        FTC_RFC_BUC,
        FTC_FOLIO,
        FTN_ID_ARCHIVO,
        FTN_NSS,
        FTC_CURP,
        FTC_RFC,
        FTC_NOMBRE_CTE,
        FTC_CLAVE_ENT_RECEP,
        FCC_VALOR_IND,
        FCN_ID_TIPO_SUBCTA,
        -- Aplicar transformaciones de TF_430_MARCA_1
        1 as FTC_IDENTIFICADOS,
        NULL as FTN_ID_DIAGNOSTICO,
        NULL as FTN_ID_SUBP_NO_CONV,
        LEFT(TRIM(FCC_VALOR_IND), 1) as FTN_VIGENCIA
    FROM #CATALOG_SCHEMA#.TEMP_UNION_IDENTIFICADOS_#SR_ID_ARCHIVO#
    ORDER BY FTN_NSS_CURP, FTN_NUM_CTA_INVDUAL
),
datos_sin_duplicados AS (
    SELECT 
        FTN_NSS_CURP,
        FTN_NUM_CTA_INVDUAL,
        FTC_NOMBRE_BUC,
        FTC_AP_PATERNO_BUC,
        FTC_AP_MATERNO_BUC,
        FTC_RFC_BUC,
        FTC_FOLIO,
        FTN_ID_ARCHIVO,
        FTN_NSS,
        FTC_CURP,
        FTC_RFC,
        FTC_NOMBRE_CTE,
        FTC_CLAVE_ENT_RECEP,
        FCC_VALOR_IND,
        FCN_ID_TIPO_SUBCTA,
        FTC_IDENTIFICADOS,
        FTN_ID_DIAGNOSTICO,
        FTN_ID_SUBP_NO_CONV,
        FTN_VIGENCIA,
        ROW_NUMBER() OVER (
            PARTITION BY FTN_NSS_CURP, FTN_NUM_CTA_INVDUAL 
            ORDER BY FTN_NSS_CURP, FTN_NUM_CTA_INVDUAL
        ) as rn
    FROM datos_ordenados
)
SELECT 
    FTN_NSS_CURP,
    FTC_IDENTIFICADOS,
    FTN_NUM_CTA_INVDUAL,
    FTN_ID_DIAGNOSTICO,
    FTN_ID_SUBP_NO_CONV,
    FTC_NOMBRE_BUC,
    FTC_AP_PATERNO_BUC,
    FTC_AP_MATERNO_BUC,
    FTC_RFC_BUC,
    FTC_FOLIO,
    FTN_ID_ARCHIVO,
    FTN_NSS,
    FTC_CURP,
    FTC_RFC,
    FTC_NOMBRE_CTE,
    FTC_CLAVE_ENT_RECEP,
    FTN_VIGENCIA,
    FCN_ID_TIPO_SUBCTA
FROM datos_sin_duplicados
WHERE rn = 1
