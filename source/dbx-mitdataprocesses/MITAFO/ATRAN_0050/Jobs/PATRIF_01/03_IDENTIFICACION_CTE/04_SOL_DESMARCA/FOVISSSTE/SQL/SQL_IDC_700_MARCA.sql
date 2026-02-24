SELECT 
  FTC_ID_SERVICIO           ,
  FTC_ID_OPERACION          ,
  FTD_FEH_PRESEN            ,
  FTC_ID_TIPO_REGIST        ,
  FTN_LINEA_CREDIT          ,
  FTN_TIPTRA                ,
  FTC_CURP                  ,
  FTC_ID_PROCESAR           ,
  FTC_NSS                   ,
  FTN_ID_ARCHIVO            ,
  FTC_NOMBRE_BUC            , 
  FTC_APELLIDO_PAT_BUC      , 
  FTC_APELLIDO_MAT_BUC      , 
  FTN_NUM_CTA_INVDUAL       ,
  FTC_NSS_BUC               , 
  FTC_CURP_BUC              ,
  FTC_RFC                   ,
  FTC_TIPO_MAR_CRE_FOV      ,
  FTC_RFC_BUC               ,
  FTC_ESTATUS               ,
  FTN_MOTIVO_RECHAZO        ,
  FTC_CORREO_ELEC           , 
  FTC_CELULAR               , 
  FTC_FOLIO                 ,
  FTD_FEH_CRE               ,
  FTC_USU_CRE               ,
  FTD_FEH_ACT               ,
  FTC_USU_ACT               ,
  FCN_ID_SUBPROCESO         ,
  FTD_FEH_CERTIFICACION     ,
  FTC_ID_INDICADOR          ,
  FTC_VIGEN_CTA 
FROM(
    SELECT
      FTC_ID_SERVICIO                  ,
      FTC_ID_OPERACION                 ,
      FTD_FEH_PRESEN                   ,
      FTC_ID_TIPO_REGIST               ,
      FTN_LINEA_CREDIT                 ,
      FTN_TIPTRA                       ,
      FTC_CURP                         ,
      FTC_ID_PROCESAR                  ,
      FTC_NSS                          ,
      NVL(FTN_ID_ARCHIVO,0) AS FTN_ID_ARCHIVO          ,
      FTC_NOMBRE_AFORE AS FTC_NOMBRE_BUC               , 
      FTC_APELLIDO_PATER_AFORE AS FTC_APELLIDO_PAT_BUC , 
      FTC_APELLIDO_MATER_AFORE AS FTC_APELLIDO_MAT_BUC , 
      FTN_NUM_CTA_INVDUAL              ,
      FTC_NSS_BUC                      , 
      CASE 
        WHEN FTN_ESTATUS_DIAG = 0 THEN NULL
        ELSE FTC_CURP END AS FTC_CURP_BUC ,
      FTC_RFC                          ,
      FTC_TIPO_MAR_CRE_FOV             ,
      FTC_RFC_BUC                      ,
      CASE
        WHEN FTN_ESTATUS_DIAG = 0 THEN 0
        ELSE 1 END AS FTC_ESTATUS      ,
      CASE 
        WHEN FTN_ESTATUS_DIAG = 0 THEN FTC_ID_DIAGNOSTICO
        ELSE NULL END AS FTN_MOTIVO_RECHAZO ,
      FTC_CORREO_ELEC                  , 
      FTN_CELULAR AS FTC_CELULAR       , 
      #SR_FOLIO# AS FTC_FOLIO          ,
      --Current_Timestamp AS FTD_FEH_CRE ,
      to_timestamp(from_utc_timestamp(CURRENT_TIMESTAMP, 'GMT-6')) AS FTD_FEH_CRE ,
      'DATABRICS' AS FTC_USU_CRE       ,
      CAST(NULL AS TIMESTAMP) AS FTD_FEH_ACT ,
      CAST(NULL AS STRING) AS FTC_USU_ACT ,
      #SR_SUBPROCESO# AS FCN_ID_SUBPROCESO ,
      TO_DATE(FTD_FECHA_CERTIFICACION,'dd/MM/yyyy') AS FTD_FEH_CERTIFICACION  ,
      FCC_VALOR_IND AS FTC_ID_INDICADOR ,
      FTC_VIGENCIA AS FTC_VIGEN_CTA     ,
      ROW_NUMBER() OVER (PARTITION BY FTC_CURP, FTN_NUM_CTA_INVDUAL 
                         ORDER BY FTC_CURP ASC, FTN_NUM_CTA_INVDUAL ASC) AS rn
    FROM #DELTA_VIV_DUP# 
) 
WHERE rn = 1
