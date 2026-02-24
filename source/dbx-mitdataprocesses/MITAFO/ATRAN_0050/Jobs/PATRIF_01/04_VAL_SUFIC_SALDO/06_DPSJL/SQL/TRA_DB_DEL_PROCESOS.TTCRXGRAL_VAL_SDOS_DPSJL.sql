-- Elimina registros del folio de suficencia de saldos antes de insertar
DELETE 
FROM PROCESOS.TTCRXGRAL_VAL_SDOS_DPSJL
WHERE
  FTC_FOLIO = '#SR_FOLIO#'