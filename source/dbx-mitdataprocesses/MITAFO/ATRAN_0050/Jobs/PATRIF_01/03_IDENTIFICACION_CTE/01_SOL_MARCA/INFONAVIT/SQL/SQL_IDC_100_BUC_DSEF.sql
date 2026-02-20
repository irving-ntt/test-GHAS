SELECT DISTINCT
   SF.FTC_CURP                              ,
   SF.FTC_NSS_BDNSAR AS FTC_NSS             ,
   B.FTC_AP_PATERNO AS FTC_AP_PATERNO_AFORE ,
   B.FTC_AP_MATERNO AS FTC_AP_MATERNO_AFORE ,
   B.FTC_NOMBRE AS FTC_NOMBRE_AFORE         ,
   '' AS FTC_RFC                            ,
   SF.FTC_FOLIO                             ,
   SF.FTN_ID_ARCHIVO                        ,
   '' AS FTC_TIPO_ARCH                      ,
   B.FTC_NSS AS FTC_NSS_BUC                 ,
   B.FTN_NUM_CTA_INVDUAL                    ,
   B.FTC_RFC AS FTC_RFC_BUC                 ,
   B.FTC_BANDERA                            ,
   B.FTN_CELULAR                            ,
   FTC_CORREO_ELEC
FROM CIERREN_ETL.TTAFOTRAS_ETL_DEV_SLD_EXC_FOV SF
LEFT JOIN CIERREN_ETL.TLSISGRAL_ETL_BUC B
     ON (SF.FTC_CURP = B.FTC_CURP)
WHERE SF.FTN_ID_ARCHIVO= '#SR_ID_ARCHIVO#'
  AND SF.FTC_FOLIO = '#SR_FOLIO#'
ORDER BY SF.FTC_CURP, B.FTN_NUM_CTA_INVDUAL