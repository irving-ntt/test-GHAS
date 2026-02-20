SELECT * FROM (
  SELECT 
    ROW_NUMBER() OVER (PARTITION BY FTC_NSS ORDER BY FTC_NSS , FC DESC) AS rn,
    FTC_IDENTIFICADOS               ,
    FTN_NUM_CTA_INVDUAL             ,
    FTN_ID_DIAGNOSTICO              ,
    FTN_ID_SUBP_NO_CONV             ,
    FTD_FECHA_CERTIFICACION         ,
    FTC_RFC_BUC                     ,
    FTC_FOLIO                       ,
    FTN_ID_ARCHIVO                  ,
    FTC_NSS                         ,
    FTC_CURP                        ,
    FTC_RFC                         ,
    FTN_VIGENCIA                    ,
    MARC_DUP                        ,
    FTC_APELLIDO_PATER_AFORE        ,
    FTC_APELLIDO_MATER_AFORE        ,
    FTC_NOMBRE_AFORE                ,
    FTC_CORREO_ELEC                 ,
    FTN_CELULAR                     
  FROM (
     SELECT 
       FTC_IDENTIFICADOS               ,
       FTN_NUM_CTA_INVDUAL             ,
       FTN_ID_DIAGNOSTICO              ,
       FTN_ID_SUBP_NO_CONV             ,
       substr(FTD_FECHA_CERTIFICACION,7,4) || substr(FTD_FECHA_CERTIFICACION,4,2) || substr(FTD_FECHA_CERTIFICACION,1,2) AS FC,
       FTD_FECHA_CERTIFICACION         ,
       FTC_RFC_BUC                     ,
       FTC_FOLIO                       ,
       FTN_ID_ARCHIVO                  ,
       FTC_NSS                         ,
       FTC_CURP                        ,
       FTC_RFC                         ,
       FTN_VIGENCIA                    ,
       MARC_DUP                        ,
       FTC_APELLIDO_PATER_AFORE        ,
       FTC_APELLIDO_MATER_AFORE        ,
       FTC_NOMBRE_AFORE                ,
       FTC_CORREO_ELEC                 ,
       FTN_CELULAR                     
     FROM #DELTA_500_CARGA#
     ORDER BY FTC_NSS DESC, FTD_FECHA_CERTIFICACION DESC
  )
)
WHERE rn = 1
  AND MARC_DUP <> 'D'
