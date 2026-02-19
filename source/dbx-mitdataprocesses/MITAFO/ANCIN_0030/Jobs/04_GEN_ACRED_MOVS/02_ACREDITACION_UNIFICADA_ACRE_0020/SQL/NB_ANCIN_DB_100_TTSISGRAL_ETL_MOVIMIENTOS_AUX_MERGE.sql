MERGE INTO CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS t
USING (
    WITH AUX AS (
        SELECT
            FTN_ID_MOV
        FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS t
        WHERE t.FTN_ID_SEMAFORO_MOV IN (190, 192)
          AND t.FTN_ID_ERROR_VAL IS NULL
          AND (
                (
                    t.FTC_FOLIO_REL = '#sr_folio#'
                    AND EXISTS (
                        SELECT 1
                        FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS
                        WHERE FTC_FOLIO_REL = '#sr_folio#'
                          AND FTN_ID_SEMAFORO_MOV IN (190, 192)
                          AND FTN_ID_ERROR_VAL IS NULL
                          AND ROWNUM = 1
                    )
                )
                OR
                (
                    t.FTC_FOLIO = '#sr_folio#'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS
                        WHERE FTC_FOLIO_REL = '#sr_folio#'
                          AND FTN_ID_SEMAFORO_MOV IN (190, 192)
                          AND FTN_ID_ERROR_VAL IS NULL
                          AND ROWNUM = 1
                    )
                )
            )
    )
    SELECT * FROM AUX
) AUX
ON (t.FTN_ID_MOV = AUX.FTN_ID_MOV)
WHEN MATCHED THEN
  UPDATE SET
       t.FTN_REG_ACREDITADO = 1,
       t.FCD_FEH_ACT        = SYSDATE,
       t.FCC_USU_ACT        = 'ACREDITACION_DATABRICKS'