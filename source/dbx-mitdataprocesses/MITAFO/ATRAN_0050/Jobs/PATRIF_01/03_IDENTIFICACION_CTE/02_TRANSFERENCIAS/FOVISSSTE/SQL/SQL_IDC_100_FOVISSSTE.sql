SELECT 
   TF.FTC_FOLIO                                            ,
   TF.FTN_ID_ARCHIVO                                       ,
   TF.FTC_NSS_TRABA AS FTC_NSS                             ,
   TF.FTC_CURP                                             ,
   TF.FTC_RFC_TRABA AS FTC_RFC                             ,
   TF.FTC_APELLIDO_PATE_BDNSAR AS FTC_APELLIDO_PATER_AFORE ,
   TF.FTC_APELLIDO_MATE_BDNSAR AS FTC_APELLIDO_MATER_AFORE ,
   TF.FTC_NOMBRE_BDNSAR AS FTC_NOMBRE_AFORE                ,
   TF.FTC_TIPO_ARCH                                        ,
   BUC.FTC_CORREO_ELEC                                     ,
   BUC.FTN_CELULAR                                         ,
   BUC.FTC_BANDERA                                         ,
   BUC.FTN_NUM_CTA_INVDUAL                                 ,
   BUC.FTC_RFC_BUC                                         ,
   BUC.FTC_NSS_BUC                                         
   
FROM  CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_FOVISSSTE TF
LEFT JOIN (
   SELECT 
     CASE WHEN B.FTC_NSS IS NULL THEN B.FTC_CURP ELSE FTC_NSS END AS FTC_NSS_CURP,
     B.FTC_NSS AS FTC_NSS_BUC,
     B.FTC_CURP FTC_CURP,
     B.FTN_NUM_CTA_INVDUAL ,
     B.FTC_NOMBRE FTC_NOMBRE_BUC,
     B.FTC_AP_PATERNO FTC_AP_PATERNO_BUC,
     B.FTC_AP_MATERNO FTC_AP_MATERNO_BUC,
     B.FTC_RFC FTC_RFC_BUC,
     B.FTC_BANDERA ,
     B.FTN_CELULAR,
     B.FTC_CORREO_ELEC
   FROM (
      SELECT FTC_CURP
      FROM  CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_FOVISSSTE
      WHERE FTC_FOLIO= '#SR_FOLIO#') A 
      LEFT JOIN CIERREN_ETL.TLSISGRAL_ETL_BUC B ON (A.FTC_CURP=B.FTC_CURP)
   WHERE B.FTC_CURP IS NOT NULL
) BUC
ON TF.FTC_CURP = BUC.FTC_CURP

WHERE TF.FTN_ID_ARCHIVO= '#SR_ID_ARCHIVO#'
  AND TF.FTC_FOLIO = '#SR_FOLIO#'
ORDER BY TF.FTC_CURP