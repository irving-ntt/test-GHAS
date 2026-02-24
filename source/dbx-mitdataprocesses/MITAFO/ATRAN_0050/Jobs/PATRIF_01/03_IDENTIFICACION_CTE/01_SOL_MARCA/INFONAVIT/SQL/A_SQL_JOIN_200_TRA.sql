SELECT DISTINCT
     TRA.FTC_FOLIO_BITACORA                                 ,
     TRA.FTN_ID_ARCHIVO                                     ,
     TRA.FTC_NSS                                            ,
     BUC.FTC_CURP_BUC AS FTC_CURP                           ,
     TRA.FTC_RFC                                            ,
     TRA.FTC_APELLIDO_PATE_INFO AS FTC_APELLIDO_PATER_AFORE ,
     TRA.FTC_APELLIDO_MATE_INFO AS FTC_APELLIDO_MATER_AFORE ,
     TRA.FTC_NOMBRES_INFO AS FTC_NOMBRE_AFORE               ,
     TRA.FTC_TIPO_ARCH                                      ,
     BUC.FTC_NSS_CURP                                       ,
     BUC.FTN_NUM_CTA_INVDUAL_BUC                            ,
     BUC.FTC_NOMBRE_BUC                                     ,
     BUC.FTC_AP_PATERNO_BUC                                 ,
     BUC.FTC_AP_MATERNO_BUC                                 ,
     BUC.FTC_RFC_BUC                                        ,
     BUC.FTC_BANDERA_BUC                                    ,
     BUC.FTN_CELULAR                                        ,
     BUC.FTC_CORREO_ELEC
FROM dbx_mit_dev_1udbvf_workspace.default.DELTA_300_TRANSFERENCIA_#SR_FOLIO# TRA
LEFT JOIN dbx_mit_dev_1udbvf_workspace.default.DELTA_200_BUC_TRA_#SR_FOLIO# BUC
     ON TRA.FTC_NSS = BUC.FTC_NSS_CURP