-- NB_PANCIN_MOV_0040_DEL_MOVS_001_DELETE.sql
-- Propósito: Eliminar registros de movimientos condicionalmente por folio o folio_rel
-- Tipo: OCI (Oracle) - DELETE condicional

DECLARE
       cuenta  NUMBER  := 0;

     BEGIN
           cuenta := 0;
           SELECT COUNT(*)
             INTO cuenta
             FROM #CX_CRE_ESQUEMA#.#TL_CRE_MOVIMIENTOS#
            WHERE FTC_FOLIO_REL = '#SR_FOLIO#' 
                  AND rownum<=1;
          IF (cuenta > 0) THEN

             DELETE /*+ PARALLEL(8) */ FROM #CX_CRE_ESQUEMA#.#TL_CRE_MOVIMIENTOS# WHERE FTC_FOLIO_REL = '#SR_FOLIO#';
             COMMIT;

          ELSE

             DELETE /*+ PARALLEL(8) */ FROM #CX_CRE_ESQUEMA#.#TL_CRE_MOVIMIENTOS# WHERE FTC_FOLIO = '#SR_FOLIO#';
             COMMIT;
          END IF;
     END; 