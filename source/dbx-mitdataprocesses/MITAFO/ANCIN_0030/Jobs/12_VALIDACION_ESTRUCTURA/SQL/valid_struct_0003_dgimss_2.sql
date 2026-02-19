WITH BASE AS (
  SELECT 
    FTN_NO_LINEA,
    FTC_CONTROL,
    FTC_LINEA,
    FTC_LONG_REG,
    SR_ID_ARCHIVO,
    FECHA_CARGA,
    VAL_ESTATUS,
    VAL_LENGTH_RECORD,

    CASE 
      WHEN FTC_CONTROL IN ('01','02','08','09') THEN FTC_CONTROL
      ELSE NULL
    END AS FTC_CONTROL_NULL
  FROM #DELTA_002#
),

PROPAGADO AS (
  SELECT
    *,
    MAX(FTC_CONTROL_NULL) OVER (
      ORDER BY FTN_NO_LINEA 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS LAST_VALID_CONTROL,
    
    LAG(FTC_CONTROL) OVER (ORDER BY FTN_NO_LINEA) AS ANT_CONTROL,
    LEAD(FTC_CONTROL) OVER (ORDER BY FTN_NO_LINEA) AS SIG_CONTROL
  FROM BASE
),

VALIDACION_SECUENCIA AS (
  SELECT
    FTN_NO_LINEA,
    FTC_CONTROL,
    SR_ID_ARCHIVO,
    FECHA_CARGA,
    FTC_LINEA,
    'Tipo de registro' AS CAMPO,
    'Error en tipo de registro' AS VALIDACION,
    0 AS COD_ERROR,
    'N/A' AS VALOR_A_VALIDAR,
    ANT_CONTROL,
    SIG_CONTROL,
    LAST_VALID_CONTROL,

    CASE
      -- Línea 1 debe ser 01
      WHEN FTN_NO_LINEA = 1 AND FTC_CONTROL_NULL IS NULL THEN 'Error en tipo de registro: 01'

      -- Si el código es inválido y el último válido fue 01 → error en detalle (02)
      WHEN FTC_CONTROL_NULL IS NULL AND LAST_VALID_CONTROL = '01' THEN 'Error en tipo de registro: 02'

      -- Si el código es inválido y el último válido fue 02 y viene otro 02, 08 o 09 → error en detalle (02)
      WHEN FTC_CONTROL_NULL IS NULL AND LAST_VALID_CONTROL = '02' AND SIG_CONTROL IN ('02', '08', '09') THEN 'Error en tipo de registro: 02'

      -- Si el código es inválido y el último válido fue 08 y viene otro 08 o 09 → error en sumario 08
      WHEN FTC_CONTROL_NULL IS NULL AND LAST_VALID_CONTROL = '08' AND SIG_CONTROL IN ('08', '09') THEN 'Error en tipo de registro: 08'

      -- Si el código es inválido y el último válido fue 08 y NO hay línea siguiente → error en sumario 09
      WHEN FTC_CONTROL_NULL IS NULL AND LAST_VALID_CONTROL = '08' AND SIG_CONTROL IS NULL THEN 'Error en tipo de registro: 09'

      -- Si el siguiente es 09 pero el anterior no fue 08 → error en sumario 08
      WHEN FTC_CONTROL_NULL IS NULL AND SIG_CONTROL = '09' AND LAST_VALID_CONTROL <> '08' THEN 'Error en tipo de registro: 08'

      -- Fallback: si el código es inválido y hay un control válido anterior
      WHEN FTC_CONTROL_NULL IS NULL AND LAST_VALID_CONTROL IS NOT NULL THEN LAST_VALID_CONTROL

      ELSE NULL
    END AS DESC_ERROR

  FROM PROPAGADO
)

SELECT *
FROM (
  SELECT
    SR_ID_ARCHIVO AS FTN_ID_ARCHIVO,
    FECHA_CARGA,
    FTC_CONTROL,
    FTN_NO_LINEA,
    CAMPO,
    VALIDACION,
    FTC_LINEA AS VALOR_CAMPO,
    COD_ERROR,
    DESC_ERROR,
    VALOR_A_VALIDAR
  FROM VALIDACION_SECUENCIA
  WHERE DESC_ERROR IS NOT NULL

  UNION ALL

  -- Error: Falta línea 1 con código '01'
  SELECT 
    SR_ID_ARCHIVO,
    FECHA_CARGA,
    NULL AS FTC_CONTROL,
    1 AS FTN_NO_LINEA,
    'Tipo de registro',
    'Error en tipo de registro',
    FTC_LINEA,
    0,
    '01',
    'N/A'
  FROM #DELTA_002#
  WHERE NOT EXISTS (
      SELECT 1
      FROM #DELTA_002#
      WHERE FTN_NO_LINEA = 1 AND FTC_CONTROL = '01'
  )

  UNION ALL

  -- Error: Última línea no es '09'
  SELECT 
    SR_ID_ARCHIVO,
    FECHA_CARGA,
    NULL AS FTC_CONTROL,
    FTN_NO_LINEA,
    'Tipo de registro',
    'Error en tipo de registro',
    FTC_LINEA,
    0,
    '09',
    'N/A'
  FROM #DELTA_002#
  WHERE FTN_NO_LINEA = (SELECT MAX(FTN_NO_LINEA) FROM #DELTA_002#)
    AND NOT EXISTS (
      SELECT 1
      FROM #DELTA_002#
      WHERE FTN_NO_LINEA = (SELECT MAX(FTN_NO_LINEA) FROM #DELTA_002#)
        AND FTC_CONTROL = '09'
    )

  UNION ALL

  -- Error: Longitud de registro inválida
  SELECT
    SR_ID_ARCHIVO,
    FECHA_CARGA,
    FTC_CONTROL,
    FTN_NO_LINEA,
    'Tipo de registro',
    'Error en tipo de registro',
    FTC_LINEA,
   0,
   '02' AS DESC_ERROR,  -- checar esto 
    'N/A'
  FROM #DELTA_002#
  WHERE VAL_LENGTH_RECORD = 0
) ERRORES_TOTALES
ORDER BY FTN_NO_LINEA

