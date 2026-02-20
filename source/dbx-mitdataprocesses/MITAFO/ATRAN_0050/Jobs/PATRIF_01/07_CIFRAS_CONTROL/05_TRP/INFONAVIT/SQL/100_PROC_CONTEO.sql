  SELECT A.FTC_FOLIO,
       NVL(A.COUNT_ESTATUS_REGISTRO,0) AS CONTEO_TOTAL,
       NVL(B.COUNT_ESTATUS_REGISTRO,0) AS CONTEO_RECHAZO
  FROM (
        SELECT FTC_FOLIO,
               Count(FTC_ESTATUS_OPE) COUNT_ESTATUS_REGISTRO
          FROM PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
         WHERE FTC_FOLIO = #SR_FOLIO#
           AND FTN_SUBPROCESO = #SR_SUBPROCESO#
      group by FTC_FOLIO
        ) A LEFT JOIN (
                       SELECT FTC_FOLIO,
                              Count(FTC_ESTATUS_OPE) COUNT_ESTATUS_REGISTRO
                         FROM PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
                        WHERE FTC_FOLIO = #SR_FOLIO#
                          AND FTN_SUBPROCESO = #SR_SUBPROCESO#
                          and FTC_ESTATUS_OPE = 02
                     group by FTC_FOLIO) B
           ON A.FTC_FOLIO=B.FTC_FOLIO
