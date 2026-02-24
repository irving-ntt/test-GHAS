SELECT DISTINCT
   DF.FTC_CURP                                  ,
   DF.FTC_NSS                                   ,
   B.FTC_AP_PATERNO AS FTC_APELLIDO_PATER_AFORE ,
   B.FTC_AP_MATERNO AS FTC_APELLIDO_MATER_AFORE ,
   B.FTC_NOMBRE AS FTC_NOMBRE_AFORE             ,
   '' AS FTC_RFC                                ,
   DF.FTC_FOLIO                                 ,
   DF.FTN_ID_ARCHIVO                            ,
   '' AS FTC_TIPO_ARCH                          ,
   B.FTC_NSS AS FTC_NSS_BUC                     ,
   B.FTN_NUM_CTA_INVDUAL                        ,
   B.FTC_RFC FTC_RFC_BUC                        ,
   B.FTC_BANDERA                                ,
   B.FTN_CELULAR                                ,
   FTC_CORREO_ELEC
FROM CIERREN_ETL.TTAFOTRAS_ETL_DESMARCA_FOVST DF
LEFT JOIN CIERREN_ETL.TLSISGRAL_ETL_BUC B 
     ON (DF.FTC_CURP = B.FTC_CURP)
WHERE DF.FTN_ID_ARCHIVO = '#SR_ID_ARCHIVO#'
  AND DF.FTC_FOLIO = '#p_SR_FOLIO#'
ORDER BY DF.FTC_CURP, B.FTN_NUM_CTA_INVDUAL