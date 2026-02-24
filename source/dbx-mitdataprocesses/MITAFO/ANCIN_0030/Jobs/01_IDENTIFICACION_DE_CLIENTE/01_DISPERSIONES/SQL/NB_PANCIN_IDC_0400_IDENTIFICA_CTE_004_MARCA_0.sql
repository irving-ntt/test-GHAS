-- Agregar campos generados por ColumnGenerator a datos rechazados del filtro
-- Equivalente al stage CG_310_MARCA_0 del job DataStage
-- En DataStage: Lee datos rechazados de FI_300_FILTRO (salida 1)

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
    FCN_ID_TIPO_SUBCTA,
    -- Campos generados por ColumnGenerator (exactamente como en DataStage)
    0 as FTC_IDENTIFICADOS,
    254 as FTN_ID_DIAGNOSTICO,
    FTN_DETALLE as FTN_ID_SUBP_NO_CONV,
    0 as FTN_VIGENCIA
FROM #CATALOG_SCHEMA#.TEMP_NO_VIGENTES_RECHAZADOS_#SR_ID_ARCHIVO#
