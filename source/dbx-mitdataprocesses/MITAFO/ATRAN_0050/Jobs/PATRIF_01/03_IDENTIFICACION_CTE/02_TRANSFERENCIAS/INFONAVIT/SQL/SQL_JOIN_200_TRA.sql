SELECT DISTINCT
     TRA.FTC_FOLIO_BITACORA AS FTC_FOLIO                                ,
     TRA.FTN_ID_ARCHIVO                                     ,
     TRA.FTC_NSS                                            ,
     BUC.FTC_CURP                           ,
     TRA.FTC_RFC                                            ,
     TRA.FTC_APELLIDO_PATE_INFO AS FTC_AP_PATER_AFORE ,
     TRA.FTC_APELLIDO_MATE_INFO AS FTC_AP_MATER_AFORE ,
     TRA.FTC_NOMBRES_INFO AS FTC_NOM_AFORE               ,
     TRA.FTC_TIPO_ARCH                                      ,
     BUC.FTC_NSS_CURP                                       ,
     BUC.FTN_NUM_CTA_INVDUAL                            ,
     BUC.FTC_NOMBRE_AFORE                                     ,
     BUC.FTC_APELLIDO_PATER_AFORE                                 ,
     BUC.FTC_APELLIDO_MATER_AFORE                                 ,
     BUC.FTC_RFC AS FTC_RFC_BUC                                      ,
     BUC.FTC_BANDERA                                        ,
     BUC.FTN_CELULAR                                        ,
     BUC.FTC_CORREO_ELEC  ,
     BUC.FTD_FEH_CRE                                  
FROM      #CATALOG_SCHEMA#.DELTA_300_TRANSFERENCIA_#SR_FOLIO# TRA
LEFT JOIN #CATALOG_SCHEMA#.DELTA_200_BUC_TRA_#SR_FOLIO# BUC
     ON TRA.FTC_NSS = BUC.FTC_NSS_CURP
