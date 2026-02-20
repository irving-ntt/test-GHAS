SELECT 
   MD.FTC_CURP                   ,
   MD.FTC_NSS_BDNSAR AS FTC_NSS  ,
   BUC.FTC_APELLIDO_PATER_AFORE  ,
   BUC.FTC_APELLIDO_MATER_AFORE  ,
   BUC.FTC_NOMBRE_AFORE          ,
   '' AS FTC_RFC                 ,
   MD.FTC_FOLIO                  ,
   MD.FTN_ID_ARCHIVO             ,
   '' AS FTC_TIPO_ARCH           ,
   BUC.FTC_NSS_BUC               ,
   BUC.FTN_NUM_CTA_INVDUAL       ,
   BUC.FTC_RFC_BUC               ,
   BUC.FTC_BANDERA               ,
   BUC.FTN_CELULAR               ,
   BUC.FTC_CORREO_ELEC
 FROM CIERREN_ETL.TTAFOTRAS_ETL_DEV_SLD_EXC_FOV MD
LEFT JOIN (
    SELECT * FROM (
      SELECT 
         CASE 
            WHEN B.FTC_NSS IS NULL THEN B.FTC_CURP 
            ELSE FTC_NSS END AS FTC_NSS_CURP             ,
         B.FTC_NSS AS FTC_NSS_BUC                        ,
         B.FTC_CURP AS FTC_CURP                          ,
         B.FTN_NUM_CTA_INVDUAL                           ,
         B.FTC_NOMBRE AS FTC_NOMBRE_AFORE                ,
         B.FTC_AP_PATERNO AS FTC_APELLIDO_PATER_AFORE    ,
         B.FTC_AP_MATERNO AS FTC_APELLIDO_MATER_AFORE    ,
         B.FTC_RFC AS FTC_RFC_BUC                        ,
         B.FTC_BANDERA                                   ,
         B.FTN_CELULAR                                   ,
         B.FTC_CORREO_ELEC                               ,
         ROW_NUMBER() OVER (PARTITION BY B.FTC_NSS, B.FTN_NUM_CTA_INVDUAL ORDER BY B.FTC_NSS, B.FTN_NUM_CTA_INVDUAL ASC) AS RN
      FROM (
           SELECT FTC_CURP
           FROM CIERREN_ETL.TTAFOTRAS_ETL_DEV_SLD_EXC_FOV
           WHERE FTC_FOLIO= '#SR_FOLIO#') A 
           LEFT JOIN CIERREN_ETL.TLSISGRAL_ETL_BUC B 
             ON (A.FTC_CURP=B.FTC_CURP)
      WHERE B.FTC_CURP IS NOT NULL
   ) 
   WHERE RN = 1
) BUC
ON MD.FTC_CURP = BUC.FTC_CURP

WHERE MD.FTN_ID_ARCHIVO = #SR_ID_ARCHIVO#
  AND MD.FTC_FOLIO = '#SR_FOLIO#'



