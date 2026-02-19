-- Elimina registros del folio de suficencia de saldos antes de insertar
DELETE 
FROM CIERREN_DATAUX.TTCRXGRAL_TRANS_INFONA_AUX 
WHERE
  FTC_FOLIO_BITACORA = '#SR_FOLIO#'