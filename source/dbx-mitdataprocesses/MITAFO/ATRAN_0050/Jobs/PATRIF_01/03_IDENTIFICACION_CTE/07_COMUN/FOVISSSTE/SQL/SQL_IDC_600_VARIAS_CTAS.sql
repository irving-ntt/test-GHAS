SELECT
   FTD_FECHA_CERTIFICACION                   ,
   FTC_NSS AS FTN_NSS                        ,
   FTN_NUM_CTA_INVDUAL                       ,
   FTC_ID_SUBP_NO_VIG                        ,
   FTN_ID_ARCHIVO                            ,
   FTC_FOLIO                                 ,
   FTC_RFC_BUC AS FTC_RFC                    ,
   FTC_ID_DIAGNOSTICO                        ,
   FTC_CURP                                  ,
   FTN_ESTATUS_DIAG                          ,
   NVL(FTC_NOMBRE_AFORE, ' ') || ' ' || NVL(FTC_APELLIDO_PATER_AFORE, ' ') || ' ' || NVL(FTC_APELLIDO_MATER_AFORE, ' ') AS FTC_NOMBRE_CTE               
                                             ,
   FTC_APELLIDO_PATER_AFORE                  ,
   FTC_APELLIDO_MATER_AFORE                  ,
   FTC_NOMBRE_AFORE                          ,
   FTC_CORREO_ELEC                           ,
   FTN_CELULAR                               ,
   FTC_NSS_BUC
FROM #DELTA_600_VARIASCTAS#