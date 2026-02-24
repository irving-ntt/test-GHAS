WITH src AS (
  SELECT
    *,
    -- vDiagnostico as numeric (100,101,104) following TF_505 logic
    CASE
      WHEN Diagnostico_97 IS NULL OR Diagnostico_08 IS NULL
        THEN TRY_CAST(COALESCE(Diagnostico_97, Diagnostico_08) AS INTEGER)
      WHEN Diagnostico_97 = Diagnostico_08
        THEN TRY_CAST(Diagnostico_97 AS INTEGER)
      WHEN Diagnostico_97 <> Diagnostico_08
        THEN 101
      ELSE 100
    END AS vDiagnostico
  FROM #FU_504_UNI_COMPLETA#
)
SELECT
  FTC_FOLIO_BITACORA AS FTC_FOLIO,
  FTN_NUM_CTA_INVDUAL,
  FTN_NO_LINEA,
  CASE
    WHEN FTN_IND_SDO_DISP_VIV08 = 1 OR FTN_IND_SDO_DISP_VIV97 = 1 THEN '01'
    ELSE '02'
  END AS FTC_ESTATUS_OPE,
  -- use numeric vDiagnostico but cast to string if target expects string; keep original name
  -- (FTC_DIAGNOSTICO in previous job was ustring; we keep same column name but return numeric as string if needed)
  CASE
    WHEN vDiagnostico IS NULL THEN NULL
    ELSE CAST(vDiagnostico AS VARCHAR(3))
  END AS FTC_DIAGNOSTICO,
  -- FTC_MOTIVO_REC: numeric mapping expanded to 104,100,101 (NULL otherwise)
  CASE
    WHEN vDiagnostico IN (104,100,101) THEN CAST(vDiagnostico AS DECIMAL(4,0))
    ELSE NULL
  END AS FTC_MOTIVO_REC,
  CURRENT_TIMESTAMP AS FTD_FEH_ACT,
  'DATABRICKS' AS FTC_USU_ACT, -- Replace with actual job user parameter
  '0' as FTN_ID_ARCHIVO,
  '0' as FTC_MASCARA_RECEP,
  'DATABRICKS' as FTC_USU_CRE,
  CURRENT_TIMESTAMP as FTD_FEH_CRE
FROM src