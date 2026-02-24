SELECT
   MD.FTN_NUM_APLI_INTE_VIVI08_SOL ,
   MD.FTC_CURP                     ,
   MD.FTC_CVE_ENTIDAD_REC_MV       ,
   MD.FTC_NOMBRE_BDNSAR            ,
   MD.FTC_CVE_ENTIDAD_EMI_MV       ,
   MD.FTN_VAL_APLI_INTE_08         ,
   MD.FTC_ORIGEN_TRANSF            ,
   MD.FTC_TIPO_REG                 ,
   MD.FTN_CONTA_SERV               ,
   MD.FTC_TIPO_ENTIDAD_EMI_MV      ,
   MD.FTC_TIPO_ENTIDAD_REC_MV      ,
   MD.FTD_FEH_PRESEN               ,
   MD.FTD_FEH_MOVIMIENTO           ,
   MD.FTC_APELLIDO_PATE_BDNSAR     ,
   MD.FTC_APELLIDO_MATE_BDNSAR     ,
   MD.FTC_IDENT_LOTE_SOLI          ,
   MD.FTC_TIPO1                    ,
   MD.FTC_TIPO2                    ,
   MD.FTN_NUM_APLI_INTE_VIVI92_SOL ,
   MD.FTN_VAL_APLI_INTE_92         ,
   MD.FTN_SALDO_TOT_VIVI92         ,
   MD.FTN_IDENTI_MOVTO             ,
   MD.FTN_SALDO_TOT_VIVI08         ,
   MD.FTC_FOLIO                    ,
   MD.FTD_FEH_VALOR_VIVI           ,
   MD.FTC_TIPO_ARCH                ,
   MD.FTD_FEH_CRE                  ,
   MD.FTC_USU_CRE                  ,
   MD.FTD_FEH_ACT                  ,
   MD.FCC_USU_ACT                  ,
   MD.FCN_ID_SUBPROCESO            ,
   MD.FTN_COIN_PROCESAR            ,
   MD.FTN_ID_ARCHIVO               ,
   VAL.FTC_RFC                     ,
   VAL.FTN_NUM_CTA_INVDUAL         ,
   VAL.FTN_ESTATUS_DIAG            ,
   VAL.FTC_ID_DIAGNOSTICO
FROM CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_FOVISSSTE MD
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
            ROW_NUMBER() OVER (PARTITION BY FTC_CURP, FTC_ID_DIAGNOSTICO ORDER BY FTN_NSS, FTC_ID_DIAGNOSTICO ASC) AS rn
          FROM CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE
          WHERE FTC_FOLIO = '#SR_FOLIO#'
         )
         WHERE rn = 1
     ) VAL
  ON MD.FTC_CURP = VAL.FTC_CURP
WHERE MD.FTC_FOLIO= '#SR_FOLIO#'
  AND MD.FTN_ID_ARCHIVO = #SR_ID_ARCHIVO#
