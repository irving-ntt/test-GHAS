SELECT DISTINCT
     QRY_200.FTC_BANDERA_BUC AS FTC_BANDERA                ,
     QRY_200.FTN_NUM_CTA_INVDUAL_BUC AS FTN_NUM_CTA_INVDUAL,
     QRY_200.FTC_RFC_BUC                                   ,
     QRY_100.FTC_FOLIO_BITACORA AS FTC_FOLIO               ,
     QRY_100.FTN_ID_ARCHIVO                                ,
     QRY_100.FTC_NSS_INFONA AS FTC_NSS                     ,
     QRY_200.FTC_CURP_BUC AS FTC_CURP                      ,
     QRY_100.FTC_RFC_AFORE AS FTC_RFC                      ,
     QRY_100.FTC_APELLIDO_PATER_AFORE                      ,
     QRY_100.FTC_APELLIDO_MATER_AFORE                      ,
     QRY_100.FTC_NOMBRE_AFORE                              ,
     QRY_100.FTC_TIPO_ARCH                                 ,
     QRY_200.FTC_CORREO_ELEC                               ,
     QRY_200.FTN_CELULAR,

     QRY_200.FTD_FEH_CRE

FROM #CATALOG_SCHEMA#.DELTA_102_MARCA_43BIS_#SR_FOLIO# QRY_100
LEFT JOIN #CATALOG_SCHEMA#.DELTA_200_BUC_354_#SR_FOLIO# QRY_200
ON QRY_100.FTC_NSS_INFONA = QRY_200.FTC_NSS