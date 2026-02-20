SELECT 
  FTC_FOLIO                                 ,
  FTN_ID_ARCHIVO                            ,
  FTC_NSS                                   ,
  FTC_CURP                                  ,
  FTC_RFC                                   ,
  FTN_NUM_CTA_INVDUAL                       ,
  1178 AS FTC_ID_DIAGNOSTICO                ,
  FTD_FECHA_CERTIFICACION                   ,
  FTN_ID_SUBP_NO_CONV AS FTN_ID_SUBP_NO_VIG ,
  0 AS FTN_ESTATUS_DIAG                     ,
  FTC_RFC_BUC                               ,
  FTN_VIGENCIA                              ,
  MARC_DUP                                  ,
  FTC_APELLIDO_PATER_AFORE                  ,
  FTC_APELLIDO_MATER_AFORE                  ,
  FTC_NOMBRE_AFORE                          ,
  FTC_IDENTIFICADOS                         ,
  FTC_CORREO_ELEC                           ,
  FTN_CELULAR                     
FROM (
   SELECT 
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
     FTN_CELULAR                     ,
     ROW_NUMBER() OVER (PARTITION BY FTC_NSS ORDER BY FTC_NSS) AS rn
   FROM #DELTA_500_CARGA#
   ORDER BY FTC_NSS DESC, FTN_NUM_CTA_INVDUAL DESC, FTN_VIGENCIA DESC, FTD_FECHA_CERTIFICACION DESC
)
WHERE MARC_DUP = 'D'
