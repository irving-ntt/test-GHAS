-- Leer datos no vigentes que NO pasan el filtro (equivalente a salida 1 de FI_300_FILTRO)
-- En DataStage: FI_300_FILTRO rechaza datos con FTN_DETALLE = 372 OR FTN_DETALLE = 212
-- Estos datos van directamente a CG_310_MARCA_0
SELECT 
    FTN_NSS_CURP,
    FTN_NUM_CTA_INVDUAL,
    FTN_ID_ARCHIVO,
    FTN_NSS,
    FTC_CURP,
    FTC_RFC,
    FTC_NOMBRE_CTE,
    FTC_FOLIO,
    FTC_NOMBRE_BUC,
    FTC_AP_PATERNO_BUC,
    FTC_AP_MATERNO_BUC,
    FTC_RFC_BUC,
    FTC_CLAVE_ENT_RECEP,
    FCC_VALOR_IND,
    FCN_ID_TIPO_SUBCTA,
    FTN_DETALLE
FROM #CATALOG_SCHEMA#.temp_novigentes_#SR_ID_ARCHIVO#
WHERE FTN_DETALLE = 372 
   OR FTN_DETALLE = 212
ORDER BY FTN_NSS_CURP
