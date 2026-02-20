SELECT
   DER.FTC_ID_SERVICIO             ,
   DER.FTC_ID_OPERACION            ,
   DER.FTC_TIPO_ENT_DES            ,
   DER.FTD_FEH_PRESEN              ,
   DER.FTN_TIPO_TRABAJADOR         ,
   DER.FTC_CURP                    ,
   DER.FTC_ID_PROCESAR             ,
   DER.FTC_NSS_BDNSAR              ,
   DER.FTN_LINEA_CRE               ,
   DER.FTN_TIPO_MOV                ,
   DER.FTN_NUM_APLI_INTE_VIVI92    ,
   DER.FTN_VAL_APLI_INTE_VIVI92    ,
   DER.FTN_SALDO_TOT_VIVI92        ,
   DER.FTN_NUM_APLI_INTE_VIVI08    ,
   DER.FTN_VAL_APLI_INTE_VIVI08    ,
   DER.FTN_SALDO_TOT_VIVI08        ,
   DER.FTD_FEH_LIQ                 ,
   DER.FTD_FEH_VALOR_VIVI          ,
   DER.FTC_FOLIO                   ,
   DER.FTN_ID_ARCHIVO              ,
   DER.FTD_FEH_CRE                 ,
   DER.FTC_USU_CRE                 ,
   DER.FCN_ID_SUBPROCESO           ,
   DER.FTC_RFC                     ,
   DER.FTN_NUM_CTA_INVDUAL         ,
   DER.FTN_ESTATUS_DIAG            ,
   DER.FTC_ID_DIAGNOSTICO          , 
   IZQ.FTC_NSS                     ,
   IZQ.FTC_APELLIDO_PATER_AFORE    , 
   IZQ.FTC_APELLIDO_MATER_AFORE    , 
   IZQ.FTC_NOMBRE_AFORE            , 
   IZQ.FTC_CORREO_ELEC             , 
   IZQ.FTN_CELULAR                 ,
   IZQ.FTC_NSS_BUC                 ,
   DER.FCN_ID_IND_CTA_INDV         ,
   DER.FFN_ID_CONFIG_INDI          , 
   DER.FCC_VALOR_IND               ,
   DER.FTC_VIGENCIA                ,
   DER.FTN_DETALLE                 ,
   DER.FTD_FEH_REG                 
FROM (
    SELECT
      MCA.FTC_ID_SERVICIO          ,
      MCA.FTC_ID_OPERACION         ,
      MCA.FTC_TIPO_ENT_DES         ,
      MCA.FTD_FEH_PRESEN           ,
      MCA.FTN_TIPO_TRABAJADOR      ,
      MCA.FTC_CURP                 ,
      MCA.FTC_ID_PROCESAR          ,
      MCA.FTC_NSS_BDNSAR           ,
      MCA.FTN_LINEA_CRE            ,
      MCA.FTN_TIPO_MOV             ,
      MCA.FTN_NUM_APLI_INTE_VIVI92 ,
      MCA.FTN_VAL_APLI_INTE_VIVI92 ,
      MCA.FTN_SALDO_TOT_VIVI92     ,
      MCA.FTN_NUM_APLI_INTE_VIVI08 ,
      MCA.FTN_VAL_APLI_INTE_VIVI08 ,
      MCA.FTN_SALDO_TOT_VIVI08     ,
      MCA.FTD_FEH_LIQ              ,
      MCA.FTD_FEH_VALOR_VIVI       ,
      MCA.FTC_FOLIO                ,
      MCA.FTN_ID_ARCHIVO           ,
      MCA.FTD_FEH_CRE              ,
      MCA.FTC_USU_CRE              ,
      MCA.FCN_ID_SUBPROCESO        ,
      MCA.FTC_RFC                  ,
      MCA.FTN_NUM_CTA_INVDUAL      ,
      MCA.FTN_ESTATUS_DIAG         ,
      MCA.FTC_ID_DIAGNOSTICO       , 
      VIV.FCN_ID_IND_CTA_INDV      ,
      VIV.FFN_ID_CONFIG_INDI       , 
      VIV.FCC_VALOR_IND            ,
      VIV.FTC_VIGENCIA             ,
      VIV.FTN_DETALLE              ,
      VIV.FTD_FEH_REG
    FROM #DELTA_VAL_IDENT# MCA
    LEFT JOIN #DELTA_VIV# VIV
      ON MCA.FTN_NUM_CTA_INVDUAL = VIV.FTN_NUM_CTA_INVDUAL
) DER
LEFT JOIN 
(  SELECT 
      FTN_NUM_CTA_INVDUAL, FTC_APELLIDO_PATER_AFORE, FTC_APELLIDO_MATER_AFORE, FTC_NOMBRE_AFORE, FTC_CORREO_ELEC, FTN_CELULAR, FTC_NSS, FTC_NSS_BUC, FTC_CURP
    FROM #DELTA_BUC#
) IZQ
ON  DER.FTC_CURP = IZQ.FTC_CURP
AND DER.FTN_NUM_CTA_INVDUAL = IZQ.FTN_NUM_CTA_INVDUAL
ORDER BY DER.FTC_CURP

