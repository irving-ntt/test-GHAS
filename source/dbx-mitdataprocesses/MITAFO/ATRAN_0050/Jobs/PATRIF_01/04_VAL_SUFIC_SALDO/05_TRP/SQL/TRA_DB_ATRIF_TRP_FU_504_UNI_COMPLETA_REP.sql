WITH base AS (
  SELECT
    FTN_NUM_CTA_INVDUAL,
    FTN_SUBPROCESO,
    FECHA_VALOR_AIVS,
    FTN_NO_LINEA,
    FTC_NSS_IMSS,
    FTN_NUM_APLI_INTE_VIVI08_SOL,
    FTN_NUM_APLI_INTE_VIVI97_SOL,
    SALDO_DISPONIBLE_AIVS_VIV97,
    SALDO_DISPONIBLE_AIVS_VIV08,
    CONTEO,
    FCN_VALOR_ACCION,
    FCN_ID_VALOR_ACCION,
    FTC_FOLIO_BITACORA,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_VIV08,
    FTN_TOTAL_AIVS_97,
    FTN_TOTAL_AIVS_08,
    ValorSaldosDisPesos_VIV97,
    ValorSaldosDisPesos_VIV08,
    FCN_ID_REGIMEN,
    FTC_INSTITUTO_ORIGEN,
    0 AS FTN_ID_ARCHIVO
  FROM  #DS_500_NSS_VAR#
),

-- TF_501_RESTA_SALDOS (Transformer) - flags numeric, cumulative subtraction loop kept as-is
tf_501 AS (
  SELECT
    *,
    CASE WHEN ValorSaldosSolPesos_VIV97 = 0 THEN 0 ELSE ValorSaldosDisPesos_VIV97 END AS SaldosVIV97,
    CASE WHEN ValorSaldosSolPesos_VIV97 = 0 OR ValorSaldosDisPesos_VIV97 <= 0 THEN 0 ELSE 1 END AS BanderaVIV97,
    CASE WHEN ValorSaldosSolPesos_VIV08 = 0 THEN 0 ELSE ValorSaldosDisPesos_VIV08 END AS SaldosVIV08,
    CASE WHEN ValorSaldosSolPesos_VIV08 = 0 OR ValorSaldosDisPesos_VIV08 <= 0 THEN 0 ELSE 1 END AS BanderaVIV08
  FROM base
),

-- FI_502_REJ (Filter) - pass only positive-flag rows forward
fi_502 AS (
  SELECT
    ValorSaldosDisPesos_VIV97,
    ValorSaldosDisPesos_VIV08,
    FTN_SUBPROCESO,
    FTC_NSS_IMSS AS FTC_NSS_TRABA_AFORE,
    FTN_NUM_CTA_INVDUAL,
    FTN_NO_LINEA,
    FTN_NUM_APLI_INTE_VIVI08_SOL,
    FTN_NUM_APLI_INTE_VIVI97_SOL,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_VIV08 AS ValorSaldosSolPesos_FOV08,
    FTN_TOTAL_AIVS_97,
    FTN_TOTAL_AIVS_08,
    FCN_VALOR_ACCION,
    FCN_ID_VALOR_ACCION,
    1 AS FTN_IND_SDO_DISP_VIV97,
    1 AS FTN_IND_SDO_DISP_VIV08,
    FTC_FOLIO_BITACORA,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    SaldosVIV97,
    BanderaVIV97,
    SaldosVIV08,
    BanderaVIV08,
    FCN_ID_REGIMEN,
    FTC_INSTITUTO_ORIGEN
  FROM tf_501
  WHERE BanderaVIV97 > 0 OR BanderaVIV08 > 0
),

-- TF_503_RESTA_SALDO (Transformer) - compute vSaldo, flags, FTN_MONTO_PESOS and produce numeric Diagnostico codes (100/101)
tf_503_prep AS (
  SELECT
    *,
    CASE WHEN SaldosVIV97 < 0 THEN 0 ELSE SaldosVIV97 END AS vSaldoSol97,
    CASE WHEN SaldosVIV08 < 0 THEN 0 ELSE SaldosVIV08 END AS vSaldoSol08
  FROM fi_502
),
tf_503 AS (
  SELECT
    FTN_NUM_CTA_INVDUAL,
    CASE WHEN (CASE WHEN ValorSaldosSolPesos_VIV97 <= vSaldoSol97 THEN ValorSaldosSolPesos_VIV97 ELSE vSaldoSol97 END) > 0 THEN 1 ELSE 0 END AS FTN_IND_SDO_DISP_VIV97,
    CASE WHEN (CASE WHEN ValorSaldosSolPesos_FOV08 <= vSaldoSol08 THEN ValorSaldosSolPesos_FOV08 ELSE vSaldoSol08 END) > 0 THEN 1 ELSE 0 END AS FTN_IND_SDO_DISP_VIV08,
    FCN_VALOR_ACCION,
    CASE WHEN ValorSaldosSolPesos_VIV97 <= vSaldoSol97 THEN ValorSaldosSolPesos_VIV97 ELSE vSaldoSol97 END AS FTN_MONTO_PESOS,
    CASE WHEN ValorSaldosSolPesos_FOV08 <= vSaldoSol08 THEN ValorSaldosSolPesos_FOV08 ELSE vSaldoSol08 END AS FTN_MONTO_PESOS_08,
    FTC_FOLIO_BITACORA,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    FTN_SUBPROCESO,
    FCN_ID_VALOR_ACCION,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_FOV08,
    FTN_TOTAL_AIVS_97,
    FTN_TOTAL_AIVS_08,
    FTN_NO_LINEA,
    FCN_ID_REGIMEN,
    -- JP_0301: Diagnostico as numeric codes (100 = total, 101 = partial)
    CASE
      WHEN ValorSaldosSolPesos_VIV97 <= vSaldoSol97 THEN 100
      WHEN ValorSaldosSolPesos_VIV97 > vSaldoSol97 THEN 101
      ELSE NULL
    END AS Diagnostico_97,
    CASE
      WHEN ValorSaldosSolPesos_FOV08 <= vSaldoSol08 THEN 100
      WHEN ValorSaldosSolPesos_FOV08 > vSaldoSol08 THEN 101
      ELSE NULL
    END AS Diagnostico_08,
    FTC_INSTITUTO_ORIGEN
  FROM tf_503_prep
),

-- DS_400_NNS_UNICO (Direct mapping) - normalize dataset Diagnostico fields to numeric if they match expected codes
ds_400 AS (
  SELECT
    FTN_NUM_CTA_INVDUAL,
    FTN_IND_SDO_DISP_VIV97,
    FTN_IND_SDO_DISP_VIV08,
    FCN_VALOR_ACCION,
    FTN_MONTO_PESOS,
    FTN_MONTO_PESOS_08,
    FTC_FOLIO_BITACORA,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    FTN_SUBPROCESO,
    FCN_ID_VALOR_ACCION,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_FOV08,
    FTN_TOTAL_AIVS_97,
    FTN_TOTAL_AIVS_08,
    FTN_NO_LINEA,
    FCN_ID_REGIMEN,
    CASE WHEN Diagnostico_97 IN ('100','101','104') THEN CAST(Diagnostico_97 AS INTEGER) ELSE NULL END AS Diagnostico_97,
    CASE WHEN Diagnostico_08 IN ('100','101','104') THEN CAST(Diagnostico_08 AS INTEGER) ELSE NULL END AS Diagnostico_08,
    FTC_INSTITUTO_ORIGEN
  FROM  #DS_400_NNS_UNICO#
),

-- DS_400_RejInsSaldos (Direct mapping) - unchanged
ds_400_rej AS (
  SELECT
    FTN_NUM_CTA_INVDUAL,
    FTN_SUBPROCESO,
    FTN_NO_LINEA,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_FOV08,
    FCN_ID_REGIMEN,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    FTC_INSTITUTO_ORIGEN
  FROM #DS_400_RejInsSaldos#
),

-- FU_401_UNION_REJ (JP_0301 rule): union DS_400_RejInsSaldos and the TF_503 rejected rows (where both flags = 0)
fu_401_union_rej AS (
  SELECT * FROM ds_400_rej
  UNION ALL
  SELECT
    FTN_NUM_CTA_INVDUAL,
    FTN_SUBPROCESO,
    FTN_NO_LINEA,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_FOV08,
    FCN_ID_REGIMEN,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    FTC_INSTITUTO_ORIGEN
  FROM tf_503
  WHERE FTN_IND_SDO_DISP_VIV97 = 0 AND FTN_IND_SDO_DISP_VIV08 = 0
),

-- CG_402_GEN_COL_FALTANTES (Column generator) - produce numeric Diagnostico = 104 per JP_0301
cg_402 AS (
  SELECT
    FTN_NUM_CTA_INVDUAL,
    0 AS FTN_IND_SDO_DISP_VIV97,
    0 AS FTN_IND_SDO_DISP_VIV08,
    0 AS FCN_VALOR_ACCION,
    0 AS FTN_MONTO_PESOS,
    0 AS FTN_MONTO_PESOS_08,
    '#sr_folio#' AS FTC_FOLIO_BITACORA,
    0 AS FCN_ID_VALOR_ACCION,
    0 AS FTN_TOTAL_AIVS_97,
    0 AS FTN_TOTAL_AIVS_08,
    104 AS Diagnostico_97,
    104 AS Diagnostico_08,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    FTN_SUBPROCESO,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_FOV08,
    FTN_NO_LINEA,
    FCN_ID_REGIMEN,
    FTC_INSTITUTO_ORIGEN
  FROM fu_401_union_rej
),

-- FU_504_UNI_COMPLETA (Funnel/Union) - keep same output columns as JP_0300 (no FTN_MOTI_RECH added)
fu_504 AS (
  SELECT
    FTN_NUM_CTA_INVDUAL,
    FTN_IND_SDO_DISP_VIV97,
    FTN_IND_SDO_DISP_VIV08,
    FCN_VALOR_ACCION,
    FTN_MONTO_PESOS,
    FTN_MONTO_PESOS_08,
    FTC_FOLIO_BITACORA,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    FTN_SUBPROCESO,
    FCN_ID_VALOR_ACCION,
    FTN_NO_LINEA,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_FOV08,
    FTN_TOTAL_AIVS_97,
    FTN_TOTAL_AIVS_08,
    FCN_ID_REGIMEN,
    Diagnostico_08,
    Diagnostico_97,
    FTC_INSTITUTO_ORIGEN
  FROM tf_503

  UNION ALL

  SELECT
    FTN_NUM_CTA_INVDUAL,
    FTN_IND_SDO_DISP_VIV97,
    FTN_IND_SDO_DISP_VIV08,
    FCN_VALOR_ACCION,
    FTN_MONTO_PESOS,
    FTN_MONTO_PESOS_08,
    FTC_FOLIO_BITACORA,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    FTN_SUBPROCESO,
    FCN_ID_VALOR_ACCION,
    FTN_NO_LINEA,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_FOV08,
    FTN_TOTAL_AIVS_97,
    FTN_TOTAL_AIVS_08,
    FCN_ID_REGIMEN,
    Diagnostico_08,
    Diagnostico_97,
    FTC_INSTITUTO_ORIGEN
  FROM ds_400

  UNION ALL

  SELECT
    FTN_NUM_CTA_INVDUAL,
    FTN_IND_SDO_DISP_VIV97,
    FTN_IND_SDO_DISP_VIV08,
    FCN_VALOR_ACCION,
    FTN_MONTO_PESOS,
    FTN_MONTO_PESOS_08,
    FTC_FOLIO_BITACORA,
    FCN_ID_TIPO_SUBCTA_97,
    FCN_ID_TIPO_SUBCTA_08,
    FTN_SUBPROCESO,
    FCN_ID_VALOR_ACCION,
    FTN_NO_LINEA,
    ValorSaldosSolPesos_VIV97,
    ValorSaldosSolPesos_FOV08,
    FTN_TOTAL_AIVS_97,
    FTN_TOTAL_AIVS_08,
    FCN_ID_REGIMEN,
    Diagnostico_08,
    Diagnostico_97,
    FTC_INSTITUTO_ORIGEN
  FROM cg_402
)

SELECT * FROM fu_504;