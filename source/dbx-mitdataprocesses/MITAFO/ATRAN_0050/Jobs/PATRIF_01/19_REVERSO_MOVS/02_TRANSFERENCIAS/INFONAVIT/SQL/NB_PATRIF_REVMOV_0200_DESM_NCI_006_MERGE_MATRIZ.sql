-- NB_PATRIF_REVMOV_0200_DESM_NCI_006_MERGE_MATRIZ.sql
-- Propósito: MERGE para actualizar matriz convivencia desde tabla auxiliar
-- Tipo: OCI (Oracle) - DML MERGE

MERGE INTO #CX_CRN_ESQUEMA#.#TL_MATRIZ_CONVIV# target
USING (
    SELECT 
        FTN_ID_MARCA,
        FTC_ESTATUS_MARCA,
        FTD_FECHA
    FROM (
        SELECT
            FTN_ID_MARCA,
            FTC_ESTATUS_MARCA,
            FTD_FECHA,
            ROW_NUMBER() OVER (
                PARTITION BY FTN_ID_MARCA
                ORDER BY FTD_FECHA DESC
            ) AS rn
        FROM #CX_DATAUX_ESQUEMA#.#TL_DATAUX_MATRIZ_CONV_AUX#
        WHERE FTC_FOLIO = #SR_FOLIO#
    )
    WHERE rn = 1   -- una sola fila por FTN_ID_MARCA
) source
ON (target.FTN_ID_MARCA = source.FTN_ID_MARCA)
WHEN MATCHED THEN
    UPDATE SET
        target.FTB_ESTATUS_MARCA = source.FTC_ESTATUS_MARCA,
        target.FTD_FEH_ACT      = source.FTD_FECHA,
        target.FCC_USU_ACT      = #CX_CRE_USUARIO# --#CX_CRE_USUARIO#
