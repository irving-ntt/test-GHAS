-- NB_PATRIF_REVMOV_0500_REV_MOVS_004_UPDATE_FULL.sql
-- ========================================================================
-- Propósito: Actualizar campos específicos preservando el resto de columnas
-- ========================================================================
-- NOTA: Se hace JOIN entre datos completos de Oracle y datos de actualización
--       Solo se actualizan 4 campos, el resto se preserva de la tabla original
--       Campos actualizados: FCN_ID_SUBPROCESO, FCD_FEH_ACT, FCC_USU_ACT, FTN_PRE_MOV_GENERADO
-- ========================================================================

MERGE INTO #CATALOG_SCHEMA#.TEMP_PRE_MOV_FULL_#SR_FOLIO# AS tgt
USING (
  SELECT *
  FROM (
    SELECT
      upd.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          upd.FTC_FOLIO,
          upd.FTN_NUM_CTA_INVDUAL,
          upd.FTN_ID_MARCA
        ORDER BY
          upd.FCD_FEH_ACT DESC NULLS LAST
      ) AS rn
    FROM #CATALOG_SCHEMA#.TEMP_ACT_PRE_MOV_#SR_FOLIO# upd
  )
  WHERE rn = 1
) AS src
ON  tgt.FTC_FOLIO           = src.FTC_FOLIO
AND tgt.FTN_NUM_CTA_INVDUAL = src.FTN_NUM_CTA_INVDUAL
AND tgt.FTN_ID_MARCA        = src.FTN_ID_MARCA

WHEN MATCHED THEN UPDATE SET
  tgt.FCN_ID_SUBPROCESO    = COALESCE(src.FCN_ID_SUBPROCESO,    tgt.FCN_ID_SUBPROCESO),
  tgt.FCD_FEH_ACT          = COALESCE(src.FCD_FEH_ACT,          tgt.FCD_FEH_ACT),
  tgt.FCC_USU_ACT          = COALESCE(src.FCC_USU_ACT,          tgt.FCC_USU_ACT),
  tgt.FTN_PRE_MOV_GENERADO = COALESCE(src.FTN_PRE_MOV_GENERADO, tgt.FTN_PRE_MOV_GENERADO)

