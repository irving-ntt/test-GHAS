SELECT DISTINCT
     TR1.FTC_BANDERA_BUC AS FTC_BANDERA                   ,
     TR1.FTN_NUM_CTA_INVDUAL_BUC AS FTN_NUM_CTA_INVDUAL   ,
     TR1.FTC_RFC_BUC                                      ,
     TM2.FTC_FOLIO                                        ,
     TM2.FTN_ID_ARCHIVO                                   ,
     TM2.FTC_NSS                                          ,
     TM2.FTC_CURP                                         ,
     ' ' AS FTC_RFC                                       ,
     TM2.FTC_APELLIDO_PATERNO AS FTC_APELLIDO_PATER_AFORE ,
     TM2.FTC_APELLIDO_MATERNO AS FTC_APELLIDO_MATER_AFORE ,
     TM2.FTC_NOMBRE AS FTC_NOMBRE_AFORE                   ,
     TR1.FTC_CORREO_ELEC                                  ,
     TR1.FTN_CELULAR                                      ,
     TM2.FTC_INSTITUTO_ORIGEN  
FROM      #CATALOG_SCHEMA#.DELTA_200_TRP_001_#SR_FOLIO# TR1
LEFT JOIN #CATALOG_SCHEMA#.DELTA_200_TMP_001_#SR_FOLIO# TM2
ON TR1.FTC_NSS = TM2.FTN_NSS