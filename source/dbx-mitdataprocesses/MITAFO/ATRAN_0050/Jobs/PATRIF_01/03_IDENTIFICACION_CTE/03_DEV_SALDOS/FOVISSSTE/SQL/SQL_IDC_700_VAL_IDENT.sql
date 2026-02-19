SELECT
   MD.FTC_ID_SERVICIO          ,
   MD.FTC_ID_OPERACION         ,
   MD.FTC_TIPO_ENT_DES         ,
   MD.FTD_FEH_PRESEN           ,
   MD.FTN_TIPO_TRABAJADOR      ,
   MD.FTC_CURP                 ,
   MD.FTC_ID_PROCESAR          ,
   MD.FTC_NSS_BDNSAR           ,
   MD.FTN_LINEA_CRE            ,
   MD.FTN_TIPO_MOV             ,
   MD.FTN_NUM_APLI_INTE_VIVI92 ,
   MD.FTN_VAL_APLI_INTE_VIVI92 ,
   MD.FTN_SALDO_TOT_VIVI92     ,
   MD.FTN_NUM_APLI_INTE_VIVI08 ,
   MD.FTN_VAL_APLI_INTE_VIVI08 ,
   MD.FTN_SALDO_TOT_VIVI08     ,
   MD.FTD_FEH_LIQ              ,
   MD.FTD_FEH_VALOR_VIVI       ,
   MD.FTC_FOLIO                ,
   MD.FTN_ID_ARCHIVO           ,
   MD.FTD_FEH_CRE              ,
   MD.FTC_USU_CRE              ,
   MD.FCN_ID_SUBPROCESO        ,
   VAL.FTC_RFC                 ,
   VAL.FTN_NUM_CTA_INVDUAL     ,
   VAL.FTN_ESTATUS_DIAG        ,
   VAL.FTC_ID_DIAGNOSTICO
FROM CIERREN_ETL.TTAFOTRAS_ETL_DEV_SLD_EXC_FOV MD
LEFT JOIN (
    SELECT 
       FTC_RFC             ,
       FTC_CURP            ,
       FTN_NUM_CTA_INVDUAL ,
       FTN_ESTATUS_DIAG    ,
       FTC_ID_DIAGNOSTICO
    FROM ( 
         SELECT 
            FTC_RFC             ,
            FTC_CURP            ,
            FTN_NUM_CTA_INVDUAL ,
            FTN_ESTATUS_DIAG    ,
            FTC_ID_DIAGNOSTICO  ,
            ROW_NUMBER() OVER (PARTITION BY FTC_CURP, FTC_ID_DIAGNOSTICO ORDER BY FTC_CURP ASC, FTC_ID_DIAGNOSTICO ASC) AS rn
          FROM CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE
          WHERE FTC_FOLIO = '#SR_FOLIO#'
         )
         WHERE rn = 1
     ) VAL
  ON MD.FTC_CURP = VAL.FTC_CURP
WHERE MD.FTC_FOLIO= '#SR_FOLIO#'
  AND MD.FTN_ID_ARCHIVO = #SR_ID_ARCHIVO#

