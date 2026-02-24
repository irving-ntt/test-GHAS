MERGE INTO CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS tgt
USING (
    WITH AUX AS (
        SELECT
            ROWID AS rid
        FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS src
        WHERE src.FTN_ID_SEMAFORO_MOV IN (190, 192)
          AND src.FTN_ID_ERROR_VAL IS NULL
          AND (
                (
                    src.FTC_FOLIO_REL = '#sr_folio#'
                    AND EXISTS (
                        SELECT 1
                        FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS x
                        WHERE x.FTC_FOLIO_REL = '#sr_folio#'
                          AND x.FTN_ID_SEMAFORO_MOV IN (190, 192)
                          AND x.FTN_ID_ERROR_VAL IS NULL
                    )
                )
                OR
                (
                    src.FTC_FOLIO = '#sr_folio#'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS x
                        WHERE x.FTC_FOLIO_REL = '#sr_folio#'
                          AND x.FTN_ID_SEMAFORO_MOV IN (190, 192)
                          AND x.FTN_ID_ERROR_VAL IS NULL
                    )
                )
          )
    )
    SELECT rid FROM AUX
) src
ON (tgt.ROWID = src.rid)
WHEN MATCHED THEN
    UPDATE SET
        tgt.FTN_MOV_GENERADO   = 1,
        tgt.FTD_ID_ESTATUS_MOV = 16,
        tgt.FCD_FEH_ACT        = SYSDATE,
        tgt.FCC_USU_ACT        = 'ACREDITACION_DATABRICKS'