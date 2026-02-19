SELECT
   FTD_FECHA_CERTIFICACION                   ,
   FTC_NSS AS FTN_NSS                        ,
   FTN_NUM_CTA_INVDUAL                       ,
   FTN_ID_SUBP_NO_CONV AS FTC_ID_SUBP_NO_VIG ,
   FTN_ID_ARCHIVO                            ,
   FTC_FOLIO                                 ,
   FTC_RFC_BUC AS FTC_RFC                    ,
   FTN_ID_DIAGNOSTICO AS FTC_ID_DIAGNOSTICO  ,
   FTC_CURP                                  ,
   FTC_IDENTIFICADOS AS FTN_ESTATUS_DIAG     ,
   NVL(FTC_NOMBRE_AFORE, ' ') || ' ' || NVL(FTC_APELLIDO_PATER_AFORE, ' ') || ' ' || NVL(FTC_APELLIDO_MATER_AFORE, ' ')   AS FTC_NOMBRE_CTE               
                                             ,
   FTC_CORREO_ELEC                           ,
   FTN_CELULAR
FROM #DELTA_600_CARGA#