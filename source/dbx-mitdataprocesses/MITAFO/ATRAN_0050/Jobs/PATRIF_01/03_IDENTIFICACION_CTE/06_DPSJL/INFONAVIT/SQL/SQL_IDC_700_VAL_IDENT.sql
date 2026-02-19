SELECT
  SJ.FTD_FEH_CRE_LOTE               ,
  SJ.FTD_FEH_LIM_RES                ,
  SJ.FTC_TIPO_REG                   ,
  SJ.FTC_IDENTI_SERV                ,
  SJ.FTN_CONSE_REG_LOTE             , 
  SJ.FTC_NSS                        ,
  SJ.FTC_CLAVE_ENT_ORIG             ,
  SJ.FTC_RFC_TRABA                  ,
  SJ.FTC_CURP                       ,
  SJ.FTC_NOMBRE                     ,
  SJ.FTN_PERIODO_PAGO               ,
  SJ.FTD_FEH_PAGO                   ,
  SJ.FTD_FEH_VALOR_RCV              ,
  SJ.FTD_FEH_VALOR_VIV              ,
  SJ.FTN_FOLIO_SUA                  ,
  SJ.FNC_CVE_ENT_RECEP              ,
  SJ.FNC_REG_PATRN_IMSS             ,
  SJ.FTN_IMP_RET_DEVOLVER           ,
  SJ.FTN_IMP_CES_CUO_PATRON_DEV     ,
  SJ.FTN_IMP_CES_CUO_TRABAJ_DEV     , 
  SJ.FTN_IMP_CUO_SOCIAL_DEV         ,
  SJ.FTN_IMP_ACT_REC_RETIRO_DEV     ,
  SJ.FTN_IMP_ACT_REC_CUO_PAT_DEV    ,
  SJ.FTN_IMP_ACT_REC_CUO_TRA_DEV    ,
  SJ.FTN_IMP_APOR_PAT_VIV_DEV       ,
  SJ.FNN_DIAS_COTZDOS_BIM_DEV       ,
  SJ.FTN_NUM_APLI_VIV_APOR_PAT_DEV  ,
  SJ.FTN_IMP_ACT_RET_REN_CUENTA     ,
  SJ.FTN_IMP_ACT_REN_CUE_IND_PAT    ,
  SJ.FTN_IMP_ACT_REN_CUE_IND_TRA    ,
  SJ.FTD_FEH_ENV_ORG_SOL_PROC       ,
  SJ.FTC_DIAG_DEV                   ,
  SJ.FTC_MOT_DEV                    ,
  SJ.FTC_RES_OPERACION              ,
  SJ.FTC_DIAGNOSTICO1               ,
  SJ.FTC_DIAGNOSTICO2               ,
  SJ.FTC_DIAGNOSTICO3               ,
  SJ.FCN_ID_SUBPROCESO              ,
  SJ.FTN_ID_ARCHIVO                 ,
  SJ.FTD_FEH_CRE                    ,
  SJ.FTC_USU_CRE                    ,
  SJ.FTC_TIPO_ARCH                  ,
  SJ.FTN_CONSEC_DIA                 ,
  SJ.FTN_NUM_REG_APORT_LOTE         ,
  SJ.FTN_NUM_REG_CTAS_PAGAR         ,
  SJ.FTC_ID_PAGO                    ,
  SJ.FTN_IMP_SOL_INSTITUTO          ,
  SJ.FTD_LIQUIDACION                ,
  SJ.FTN_IMP_APACEP_RCV_DEV_ADM     ,
  SJ.FTN_IMP_APEND_RCV_ADM          ,
  SJ.FTN_IMP_APRECH_RCV_ADM         ,
  SJ.FTN_NUM_APLI_INT_VIV_SOL_INST  ,
  SJ.FTN_NUM_APLI_INT_VIV_ACDEV_ADM ,
  SJ.FTN_NUM_APLI_INT_VIV_PEND_ADM  ,
  SJ.FTN_NUM_APLI_INT_VIV_RECH_ADM  ,
  SJ.FTN_IMP_VIV_ACEP_DEV_ADM       ,
  SJ.FTN_IMP_VIV_PEND_DEV_ADM       ,
  SJ.FTN_IMP_VIV_RECH_DEV_ADM       ,
  SJ.FTC_IND_SIEFORE                ,
  SJ.FTC_TIPO_SIEFORE               ,
  VAL.FTC_RFC                       ,
  VAL.FTC_CURP_IDC                  ,
  VAL.FTN_NUM_CTA_INVDUAL           ,
  VAL.FTN_ESTATUS_DIAG              ,
  VAL.FTC_ID_DIAGNOSTICO
  
FROM CIERREN_ETL.TTAFOTRAS_ETL_DEV_PAG_SJL SJ
LEFT JOIN (
    SELECT 
       FTN_NSS             ,
       FTC_RFC             ,
       FTC_CURP_IDC        ,
       FTN_NUM_CTA_INVDUAL ,
       FTN_ESTATUS_DIAG    ,
       FTC_ID_DIAGNOSTICO
    FROM ( 
         SELECT 
            FTN_NSS                  ,
            FTC_RFC                  ,
            FTC_CURP AS FTC_CURP_IDC ,
            FTN_NUM_CTA_INVDUAL ,
            FTN_ESTATUS_DIAG    ,
            FTC_ID_DIAGNOSTICO  ,
            ROW_NUMBER() OVER (PARTITION BY FTN_NSS, FTC_ID_DIAGNOSTICO ORDER BY FTN_NSS, FTC_ID_DIAGNOSTICO ASC) AS rn
          FROM CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE
          WHERE FTC_FOLIO = '#SR_FOLIO#'
         )
         WHERE rn = 1
     ) VAL
  ON SJ.FTC_NSS = VAL.FTN_NSS
WHERE SJ.FTC_FOLIO = '#SR_FOLIO#'
  AND SJ.FTN_ID_ARCHIVO = #SR_ID_ARCHIVO#
  AND SJ.FTC_TIPO_REG IN ('02')

