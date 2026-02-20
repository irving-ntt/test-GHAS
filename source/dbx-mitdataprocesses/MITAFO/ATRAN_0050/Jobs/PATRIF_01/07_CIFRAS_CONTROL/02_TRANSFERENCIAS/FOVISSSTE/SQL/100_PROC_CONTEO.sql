SELECT A.FTC_FOLIO,
       NVL(A.COUNT_ESTATUS_REGISTRO,0) AS CONTEO_TOTAL,
       NVL(B.COUNT_ESTATUS_REGISTRO,0) AS CONTEO_RECHAZO
  FROM (
        SELECT FTC_FOLIO,
               Count(FTC_ESTATUS) COUNT_ESTATUS_REGISTRO
          FROM PROCESOS.TTAFOTRAS_TRANS_FOVISSSTE
         WHERE FTC_FOLIO = #SR_FOLIO#
           AND FCN_ID_SUBPROCESO = #SR_SUBPROCESO#
      group by FTC_FOLIO
        ) A LEFT JOIN (
                       SELECT FTC_FOLIO,
                              Count(FTC_ESTATUS) COUNT_ESTATUS_REGISTRO
                         FROM PROCESOS.TTAFOTRAS_TRANS_FOVISSSTE
                        WHERE FTC_FOLIO = #SR_FOLIO#
                          AND FCN_ID_SUBPROCESO = #SR_SUBPROCESO#
                          and FTC_ESTATUS=0
                     group by FTC_FOLIO) B
           ON A.FTC_FOLIO=B.FTC_FOLIO
