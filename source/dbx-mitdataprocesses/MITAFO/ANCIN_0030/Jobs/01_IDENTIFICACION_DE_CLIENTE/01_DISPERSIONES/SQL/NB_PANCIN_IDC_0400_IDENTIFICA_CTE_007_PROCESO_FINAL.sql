-- Proceso final: Conteo + Join + Filtro + Marcas + Unión
-- Equivalente a CP_600 + AG_700 + JO_800 + FL_810 + CL_820/830 + FU_900 del job DataStage
-- En DataStage:
-- - CP_600_VIG: Copy (no hace nada)
-- - AG_700_COUNT: Cuenta por FTN_NSS_CURP, suma FTN_VIGENCIA
-- - JO_800_VIGENCIA: Left join datos originales + conteo
-- - FL_810_VIGENCIA: Filtro con 2 salidas (CUENTASVIG > 1 AND FTN_VIGENCIA = 1) vs (CUENTASVIG < 2)
-- - CL_820_MARCADUP: Agrega MARC_DUP = 'D'
-- - CL_830_MARCADUPN: Agrega MARC_DUP = ''
-- - FU_900_CTAS: Une las dos salidas

WITH conteo_vigencia AS (
    -- AG_700_COUNT: Contar por FTN_NSS_CURP, sumar FTN_VIGENCIA
    SELECT 
        FTN_NSS_CURP,
        SUM(FTN_VIGENCIA) as CUENTASVIG
    FROM #CATALOG_SCHEMA#.TEMP_UNION_FINAL_#SR_ID_ARCHIVO#
    GROUP BY FTN_NSS_CURP
),
datos_con_conteo AS (
    -- JO_800_VIGENCIA: Left join datos originales + conteo
    SELECT 
        d.FTN_NSS_CURP,
        d.FTC_IDENTIFICADOS,
        d.FTN_NUM_CTA_INVDUAL,
        d.FTN_ID_DIAGNOSTICO,
        d.FTN_ID_SUBP_NO_CONV,
        d.FTC_NOMBRE_BUC,
        d.FTC_AP_PATERNO_BUC,
        d.FTC_AP_MATERNO_BUC,
        d.FTC_RFC_BUC,
        d.FTC_FOLIO,
        d.FTN_ID_ARCHIVO,
        d.FTN_NSS,
        d.FTC_CURP,
        d.FTC_RFC,
        d.FTC_NOMBRE_CTE,
        d.FTC_CLAVE_ENT_RECEP,
        d.FTN_VIGENCIA,
        d.FCN_ID_TIPO_SUBCTA,
        COALESCE(c.CUENTASVIG, 0) as CUENTASVIG
    FROM #CATALOG_SCHEMA#.TEMP_UNION_FINAL_#SR_ID_ARCHIVO# d
    LEFT JOIN conteo_vigencia c ON d.FTN_NSS_CURP = c.FTN_NSS_CURP
),
datos_filtrados_dup AS (
    -- FL_810_VIGENCIA salida 0: CUENTASVIG > 1 AND FTN_VIGENCIA = 1
    -- CL_820_MARCADUP: Agrega MARC_DUP = 'D'
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
        CUENTASVIG,
        FCN_ID_TIPO_SUBCTA,
        'D' as MARC_DUP
    FROM datos_con_conteo
    WHERE CUENTASVIG > 1 AND FTN_VIGENCIA = 1
),
datos_filtrados_nodup AS (
    -- FL_810_VIGENCIA salida 1: CUENTASVIG < 2
    -- CL_830_MARCADUPN: Agrega MARC_DUP = ''
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
        CUENTASVIG,
        FCN_ID_TIPO_SUBCTA,
        '' as MARC_DUP
    FROM datos_con_conteo
    WHERE CUENTASVIG < 2
)
-- FU_900_CTAS: Unir las dos salidas
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
    CUENTASVIG,
    FCN_ID_TIPO_SUBCTA,
    MARC_DUP
FROM datos_filtrados_dup

UNION ALL

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
    CUENTASVIG,
    FCN_ID_TIPO_SUBCTA,
    MARC_DUP
FROM datos_filtrados_nodup
