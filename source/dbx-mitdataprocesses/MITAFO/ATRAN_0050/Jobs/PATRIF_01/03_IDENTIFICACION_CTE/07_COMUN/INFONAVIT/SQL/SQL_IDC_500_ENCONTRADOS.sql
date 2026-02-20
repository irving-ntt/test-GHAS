SELECT 
  ENC.FTC_IDENTIFICADOS        ,
  ENC.FTN_NUM_CTA_INVDUAL      ,
  ENC.FTN_ID_DIAGNOSTICO       ,
  ENC.FTN_ID_SUBP_NO_CONV      ,
  FCH.FTD_FECHA_CERTIFICACION  ,
  NULL AS FTN_CTE_PENSIONADO   ,
  ENC.FTC_RFC_BUC              ,
  ENC.FTC_FOLIO                ,
  ENC.FTN_ID_ARCHIVO           ,
  ENC.FTC_NSS                  ,
  ENC.FTC_CURP                 ,
  ENC.FTC_RFC                  ,
  ENC.FTN_VIGENCIA             ,
  ENC.MARC_DUP                 ,
  ENC.FTC_APELLIDO_PATER_AFORE ,
  ENC.FTC_APELLIDO_MATER_AFORE ,
  ENC.FTC_NOMBRE_AFORE         ,
  ENC.FTC_CORREO_ELEC          ,
  ENC.FTN_CELULAR
FROM (
   SELECT 
     FTC_IDENTIFICADOS        ,
     FTN_NUM_CTA_INVDUAL      ,
     FTN_ID_DIAGNOSTICO       ,
     FTN_ID_SUBP_NO_CONV      ,
     FTC_RFC_BUC              ,
     FTC_FOLIO                ,
     FTN_ID_ARCHIVO           ,
     FTC_NSS                  ,
     FTC_CURP                 ,
     FTC_RFC                  ,
     FTN_VIGENCIA             ,
     MARC_DUP                 ,
     FTC_APELLIDO_PATER_AFORE ,
     FTC_APELLIDO_MATER_AFORE ,
     FTC_NOMBRE_AFORE         ,
     FTC_CORREO_ELEC          ,
     FTN_CELULAR
   FROM #DELTA_TABLA_ENCONTRADOS#
   ORDER BY FTN_NUM_CTA_INVDUAL
) ENC
LEFT JOIN (
   SELECT 
     FTN_NUM_CTA_INVDUAL      ,
     FCC_VALOR_IND AS FTD_FECHA_CERTIFICACION
   FROM #DELTA_TABLA_FECHA#
   ORDER BY FTN_NUM_CTA_INVDUAL
) FCH
ON ENC.FTN_NUM_CTA_INVDUAL = FCH.FTN_NUM_CTA_INVDUAL
