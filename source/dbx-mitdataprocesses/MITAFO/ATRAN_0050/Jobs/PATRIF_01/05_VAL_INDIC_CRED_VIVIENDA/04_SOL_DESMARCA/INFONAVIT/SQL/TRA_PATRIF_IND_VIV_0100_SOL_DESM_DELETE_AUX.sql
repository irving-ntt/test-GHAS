
-- Elimina registros del folio de suficencia de saldos antes de insertar
DELETE 
FROM CIERREN_DATAUX.TTCRXGRAL_MARCA_DESMARCA_INFO_AUX
WHERE
  FTC_FOLIO_BITACORA = '#sr_folio#'