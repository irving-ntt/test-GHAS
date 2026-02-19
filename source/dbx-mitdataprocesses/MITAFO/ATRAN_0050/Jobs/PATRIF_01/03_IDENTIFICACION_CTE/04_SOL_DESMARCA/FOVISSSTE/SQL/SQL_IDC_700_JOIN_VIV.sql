SELECT
  DER.FTC_ID_SERVICIO              ,
  DER.FTC_ID_OPERACION             ,
  DER.FTD_FEH_PRESEN               ,
  DER.FTC_ID_TIPO_REGIST           ,
  DER.FTN_LINEA_CREDIT             ,
  DER.FTN_TIPTRA                   ,
  DER.FTC_CURP                     ,
  DER.FTC_ID_PROCESAR              ,
  DER.FTC_NSS                      ,
  DER.FTN_ID_ARCHIVO               ,
  DER.FTC_RFC                      ,
  DER.FTC_TIPO_MAR_CRE_FOV         ,
  DER.FTC_RFC_BUC                  ,
  DER.FTN_NUM_CTA_INVDUAL          ,
  DER.FTN_ESTATUS_DIAG             ,
  DER.FTC_ID_DIAGNOSTICO           ,
  IZQ.FTC_APELLIDO_PATER_AFORE     , 
  IZQ.FTC_APELLIDO_MATER_AFORE     , 
  IZQ.FTC_NOMBRE_AFORE             , 
  IZQ.FTC_CORREO_ELEC              , 
  IZQ.FTN_CELULAR                  , 
  IZQ.FTC_NSS_BUC                  , 
  IZQ.FTD_FECHA_CERTIFICACION      ,
  DER.FCC_VALOR_IND                ,
  DER.FCN_ID_IND_CTA_INDV          ,
  DER.FFN_ID_CONFIG_INDI           ,
  DER.FTC_VIGENCIA                 ,
  DER.FTN_DETALLE                  ,
  DER.FTD_FEH_REG
FROM (
    SELECT
      MCA.FTC_ID_SERVICIO           ,
      MCA.FTC_ID_OPERACION          ,
      MCA.FTD_FEH_PRESEN            ,
      MCA.FTC_ID_TIPO_REGIST        ,
      MCA.FTN_LINEA_CREDIT          ,
      MCA.FTN_TIPTRA                ,
      MCA.FTC_CURP                  ,
      MCA.FTC_ID_PROCESAR           ,
      MCA.FTC_NSS                   ,
      MCA.FTN_ID_ARCHIVO            ,
      MCA.FTC_RFC                   ,
      MCA.FTC_TIPO_MAR_CRE_FOV      ,
      MCA.FTC_RFC_BUC               ,
      MCA.FTN_NUM_CTA_INVDUAL       ,
      MCA.FTN_ESTATUS_DIAG          ,
      MCA.FTC_ID_DIAGNOSTICO        ,
      VIV.FCN_ID_IND_CTA_INDV       ,
      VIV.FFN_ID_CONFIG_INDI        ,
      VIV.FCC_VALOR_IND             ,
      VIV.FTC_VIGENCIA              ,
      VIV.FTN_DETALLE               ,
      VIV.FTD_FEH_REG
    FROM #DELTA_VAL_IDENT# MCA
    LEFT JOIN #DELTA_VIV# VIV
      ON MCA.FTN_NUM_CTA_INVDUAL = VIV.FTN_NUM_CTA_INVDUAL
) DER
LEFT JOIN 
(  SELECT 
      FTC_APELLIDO_PATER_AFORE, FTC_APELLIDO_MATER_AFORE, FTC_NOMBRE_AFORE, FTC_CORREO_ELEC, FTN_CELULAR, FTC_NSS_BUC, FTC_CURP, FTN_NUM_CTA_INVDUAL, FTD_FECHA_CERTIFICACION
    FROM #DELTA_BUC#
) IZQ
ON  DER.FTC_CURP = IZQ.FTC_CURP
AND DER.FTN_NUM_CTA_INVDUAL = IZQ.FTN_NUM_CTA_INVDUAL
ORDER BY DER.FTC_CURP
