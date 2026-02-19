-- Elimina registros del folio de suficencia de saldos antes de insertar
DELETE 
FROM CIERREN_DATAUX.TTAFOTRAS_TRANS_REC_PORTA_AUX
WHERE
  FTC_FOLIO = '#SR_FOLIO#'