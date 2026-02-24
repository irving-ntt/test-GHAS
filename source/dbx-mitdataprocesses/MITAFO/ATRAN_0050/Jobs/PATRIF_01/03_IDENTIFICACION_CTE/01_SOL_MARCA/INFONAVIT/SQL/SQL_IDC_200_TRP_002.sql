SELECT DISTINCT
    B.FTC_CURP                                    ,
    B.FTN_NUM_CTA_INVDUAL FTN_NUM_CTA_INVDUAL_BUC ,
    B.FTC_RFC FTC_RFC_BUC                         ,
    B.FTC_BANDERA FTC_BANDERA_BUC                 ,
    B.FTC_CORREO_ELEC                             ,
    B.FTN_CELULAR
FROM (
           SELECT FTC_NSS_IMSS,FTC_CURP
            FROM CIERREN_ETL.TTAFOTRAS_ETL_TRANS_REC_PORTA
           WHERE FTC_FOLIO = '#SR_FOLIO#'
           AND FTC_INSTITUTO_ORIGEN = '002') A 
           LEFT JOIN CIERREN_ETL.TLSISGRAL_ETL_BUC B ON
           A.FTC_CURP = B.FTC_CURP