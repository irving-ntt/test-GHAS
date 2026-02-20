SELECT DISTINCT
   TF.FTC_FOLIO                                            ,
   TF.FTN_ID_ARCHIVO                                       ,
   TF.FTC_NSS_TRABA AS  FTC_NSS                            ,
   TF.FTC_CURP                                             ,
   TF.FTC_RFC_TRABA AS FTC_RFC                             ,
   TF.FTC_APELLIDO_PATE_BDNSAR AS FTC_APELLIDO_PATER_AFORE ,
   TF.FTC_APELLIDO_MATE_BDNSAR AS FTC_APELLIDO_MATER_AFORE ,
   TF.FTC_NOMBRE_BDNSAR AS FTC_NOMBRE_AFORE                ,
   TF.FTC_TIPO_ARCH                                        ,
   B.FTC_CORREO_ELEC                                       ,
   B.FTN_CELULAR                                           ,
   B.FTC_BANDERA                                           ,
   B.FTN_NUM_CTA_INVDUAL                                   ,
   B.FTC_RFC AS FTC_RFC_BUC                                ,
   B.FTC_NSS AS FTC_NSS_BUC
FROM  CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_FOVISSSTE TF
LEFT JOIN CIERREN_ETL.TLSISGRAL_ETL_BUC B
     ON (TF.FTC_CURP = B.FTC_CURP)
WHERE  TF.FTN_ID_ARCHIVO='#SR_ID_ARCHIVO#'
   AND TF.FTC_FOLIO = '#SR_FOLIO#'
   AND B.FTC_CURP IS NOT NULL