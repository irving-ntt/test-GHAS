MERGE INTO CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS AS tgt
USING (
    SELECT DISTINCT ROW_ID
    FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS m
    WHERE m.FTN_ID_SEMAFORO_MOV IN (190, 192)
      AND m.FTN_ID_ERROR_VAL IS NULL
      AND (
            (
              EXISTS (
                SELECT 1
                FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS x
                WHERE x.FTC_FOLIO_REL = '202401041208132825'
                  AND x.FTN_ID_SEMAFORO_MOV IN (190,192)
                  AND x.FTN_ID_ERROR_VAL IS NULL
              )
              AND m.FTC_FOLIO_REL = '202401041208132825'
            )
            OR
            (
              NOT EXISTS (
                SELECT 1
                FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS x
                WHERE x.FTC_FOLIO_REL = '202401041208132825'
                  AND x.FTN_ID_SEMAFORO_MOV IN (190,192)
                  AND x.FTN_ID_ERROR_VAL IS NULL
              )
              AND m.FTC_FOLIO = '202401041208132825'
            )
          )
) src
ON tgt.ROW_ID = src.ROW_ID
WHEN MATCHED THEN
  UPDATE SET
      tgt.FTN_REG_ACREDITADO = '1',
      tgt.FCD_FEH_ACT        = current_timestamp(),
      tgt.FCC_USU_ACT        = 'ACREDITACION_DATABRICKS'