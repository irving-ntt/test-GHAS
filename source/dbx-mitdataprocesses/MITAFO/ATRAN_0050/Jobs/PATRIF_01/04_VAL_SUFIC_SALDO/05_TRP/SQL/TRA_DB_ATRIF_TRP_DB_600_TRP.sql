SELECT
  FTC_FOLIO_BITACORA AS FTC_FOLIO,
  FTN_NUM_CTA_INVDUAL,
  FTN_NO_LINEA,
  CASE
    WHEN FTN_IND_SDO_DISP_VIV08 = 1 OR FTN_IND_SDO_DISP_VIV97 = 1 THEN '01'
    ELSE '02'
  END AS FTC_ESTATUS_OPE,
  -- vDiagnostico logic
  CASE
    WHEN Diagnostico_97 IS NULL OR Diagnostico_08 IS NULL THEN
      COALESCE(Diagnostico_97, Diagnostico_08)
    WHEN Diagnostico_97 = Diagnostico_08 THEN Diagnostico_97
    WHEN Diagnostico_97 <> Diagnostico_08 THEN '101'
    ELSE '100'
  END AS FTC_DIAGNOSTICO,
  -- FTC_MOTIVO_REC logic
  CASE
    WHEN (
      CASE
        WHEN Diagnostico_97 IS NULL OR Diagnostico_08 IS NULL THEN
          COALESCE(Diagnostico_97, Diagnostico_08)
        WHEN Diagnostico_97 = Diagnostico_08 THEN Diagnostico_97
        WHEN Diagnostico_97 <> Diagnostico_08 THEN '101'
        ELSE '100'
      END
    ) = '104' THEN 104
    ELSE NULL
  END AS FTC_MOTIVO_REC,
  CURRENT_TIMESTAMP AS FTD_FEH_ACT,
  'DATABRICKS' AS FTC_USU_ACT, -- Replace with actual job user parameter
  '0' as FTN_ID_ARCHIVO,
  '0' as FTC_MASCARA_RECEP,
  'DATABRICKS' as FTC_USU_CRE,
  CURRENT_TIMESTAMP as FTD_FEH_CRE
FROM #FU_504_UNI_COMPLETA#