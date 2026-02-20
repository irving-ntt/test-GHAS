SELECT
  FTC_NSS                ,
  FTC_CURP_BUC           ,
  FTN_NUM_CTA_INVDUAL_BUC,
  FTC_NOMBRE_BUC         ,
  FTC_AP_PATERNO_BUC     ,
  FTC_AP_MATERNO_BUC     ,
  FTC_RFC_BUC            ,
  FTC_BANDERA_BUC        ,
  FTN_CELULAR            ,
  FTC_CORREO_ELEC                               
FROM(
  SELECT 
    B.FTC_NSS AS FTC_NSS                            ,
    B.FTC_CURP AS FTC_CURP_BUC                      ,
    B.FTN_NUM_CTA_INVDUAL AS FTN_NUM_CTA_INVDUAL_BUC,
    B.FTC_NOMBRE AS FTC_NOMBRE_BUC                  ,
    B.FTC_AP_PATERNO AS FTC_AP_PATERNO_BUC          ,
    B.FTC_AP_MATERNO AS FTC_AP_MATERNO_BUC          ,
    B.FTC_RFC AS FTC_RFC_BUC                        ,
    B.FTC_BANDERA AS FTC_BANDERA_BUC                ,
    B.FTN_CELULAR                                   ,
    B.FTC_CORREO_ELEC                               ,
    ROW_NUMBER() OVER (PARTITION BY B.FTC_NSS, B.FTN_NUM_CTA_INVDUAL ORDER BY B.FTC_NSS, B.FTN_NUM_CTA_INVDUAL ASC) AS rn
  FROM (
           SELECT FTC_NSS_IMSS,FTC_CURP
           FROM CIERREN_ETL.TTAFOTRAS_ETL_TRANS_REC_PORTA
           WHERE FTC_FOLIO='#SR_FOLIO#'
             AND FTC_INSTITUTO_ORIGEN in ('001','002') ) A 
           LEFT JOIN CIERREN_ETL.TLSISGRAL_ETL_BUC B 
             ON A.FTC_NSS_IMSS=B.FTC_NSS
  )

