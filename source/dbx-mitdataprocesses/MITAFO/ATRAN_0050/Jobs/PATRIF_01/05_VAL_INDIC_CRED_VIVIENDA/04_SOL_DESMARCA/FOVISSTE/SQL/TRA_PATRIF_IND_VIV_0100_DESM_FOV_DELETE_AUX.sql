
-- Elimina registros del folio de suficencia de saldos antes de insertar
DELETE 
FROM CIERREN_DATAUX.TTAFOTRAS_DESMARCA_FOVST_AUX
WHERE
  FTC_FOLIO = '#sr_folio#'