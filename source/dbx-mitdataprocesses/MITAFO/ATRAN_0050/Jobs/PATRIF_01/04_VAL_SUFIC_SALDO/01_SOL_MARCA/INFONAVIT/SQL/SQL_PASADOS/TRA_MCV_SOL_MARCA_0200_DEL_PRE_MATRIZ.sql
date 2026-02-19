-- Elimina registros del folio de pre matriz antes de insertar
DELETE 
FROM CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
WHERE
  FTC_FOLIO = '#SR_FOLIO#'