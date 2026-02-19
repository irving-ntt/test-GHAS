SELECT DISTINCT
     D2.FTC_BANDERA_BUC AS FTC_BANDERA                     ,
     D2.FTN_NUM_CTA_INVDUAL_BUC AS FTN_NUM_CTA_INVDUAL     ,
     D2.FTC_RFC_BUC                                        ,
     D3.FTC_FOLIO                                          ,
     D3.FTN_ID_ARCHIVO                                     ,
     D3.FTC_NSS                                            ,
     D3.FTC_CURP                                           ,
     D3.FTC_RFC_TRABA AS FTC_RFC                           ,
     D3.FTC_APELLIDO_PATE_INFO AS FTC_APELLIDO_PATER_AFORE ,
     D3.FTC_APELLIDO_MATE_INFO AS FTC_APELLIDO_MATER_AFORE ,
     D3.FTC_NOMBRE AS FTC_NOMBRE_AFORE                     ,
     D3.FTC_TIPO_ARCH                                      ,
     D2.FTN_CELULAR                                        ,
     D2.FTC_CORREO_ELEC
FROM #CATALOG_SCHEMA#.DELTA_300_DPSJL_#SR_FOLIO# D3
LEFT JOIN #CATALOG_SCHEMA#.DELTA_200_DPSJL_#SR_FOLIO# D2
ON D3.FTC_NSS = D2.FTC_NSS_CURP