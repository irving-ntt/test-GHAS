      MERGE INTO CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS t
USING (
    SELECT *
    FROM (
        SELECT
            m.*,
            COALESCE(m.FTC_FOLIO_REL, m.FTC_FOLIO) AS MERGE_FOLIO,
            ROW_NUMBER() OVER (
                PARTITION BY COALESCE(m.FTC_FOLIO_REL, m.FTC_FOLIO)
                ORDER BY m.FCD_FEH_CRE DESC
            ) AS rn
        FROM CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS m
        WHERE m.FTC_FOLIO_REL = '#sr_folio#'
       OR m.FTC_FOLIO     = '#sr_folio#'
    )
    WHERE rn = 1
) src
ON (
       t.FTC_FOLIO_REL = src.MERGE_FOLIO
    OR t.FTC_FOLIO     = src.MERGE_FOLIO
)
WHEN MATCHED THEN
UPDATE SET
    t.FTN_PRE_MOV_GENERADO = 2,
    t.FCD_FEH_ACT          = SYSDATE,
    t.FCC_USU_ACT          = 'ACREDITACION_DATABRICKS'