-- Elimina registros del folio de suficencia de saldos antes de insertar
DELETE 
FROM PROCESOS.TTSISGRAL_SUF_SALDOS
WHERE
  FTC_FOLIO_BITACORA = '#SR_FOLIO#'