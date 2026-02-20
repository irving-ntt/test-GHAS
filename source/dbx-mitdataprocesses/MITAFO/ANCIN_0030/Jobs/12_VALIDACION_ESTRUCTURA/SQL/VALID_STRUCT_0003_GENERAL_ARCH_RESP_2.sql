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
      WHEN FTC_CONTROL IN ('01','02','09') THEN FTC_CONTROL
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
    
    LAG(FTC_CONTROL_NULL) OVER (ORDER BY FTN_NO_LINEA) AS ANT_CONTROL_NULL,
    LEAD(FTC_CONTROL_NULL) OVER (ORDER BY FTN_NO_LINEA) AS SIG_CONTROL_NULL
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
    ANT_CONTROL_NULL,
    SIG_CONTROL_NULL,
    LAST_VALID_CONTROL,

    CASE
  -- Prioridad: si tiene error de longitud, no validar tipo de registro 
  WHEN VAL_LENGTH_RECORD = 0 THEN NULL

   -- Si la primera línea no es 01 → error encabezado
  WHEN FTN_NO_LINEA = 1 AND FTC_CONTROL_NULL IS NULL THEN 'Error en encabezado: 01'
  
  -- Si existe un 01 fuera de la primera línea → error detalle 02
  WHEN FTC_CONTROL = '01' AND FTN_NO_LINEA > 1 THEN 'Error en tipo de registro: 02'

  -- Si error y línea anterior = 01 → error detalle 02
  WHEN FTC_CONTROL_NULL IS NULL AND ANT_CONTROL_NULL = '01' THEN 'Error en tipo de registro: 02'

  -- Si error y línea anterior = 02 y (línea siguiente = 02 o 09) → error detalle 02
  WHEN FTC_CONTROL_NULL IS NULL AND ANT_CONTROL_NULL = '02' AND SIG_CONTROL_NULL IN ('02','09') THEN 'Error en tipo de registro: 02'

  -- Si error y línea anterior = 02 y además está en la última línea del archivo → error sumario 09
  WHEN FTC_CONTROL_NULL IS NULL 
       AND ANT_CONTROL_NULL = '02'
       AND FTN_NO_LINEA = (SELECT MAX(FTN_NO_LINEA) FROM #DELTA_002#)
       THEN 'Error en sumario registro: 09'

  -- Si la línea actual es la última y no es 09 → error en sumario 09
  WHEN FTC_CONTROL_NULL IS NULL 
       AND FTN_NO_LINEA = (SELECT MAX(FTN_NO_LINEA) FROM #DELTA_002#)
       THEN 'Error en sumario registro: 09'

  -- Cualquier otro error → detalle 02
  WHEN FTC_CONTROL_NULL IS NULL THEN 'Error en tipo de registro: 02'

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

-- Falta línea con código '01'
SELECT 
  SR_ID_ARCHIVO,
  FECHA_CARGA,
  NULL AS FTC_CONTROL,
  (SELECT MIN(FTN_NO_LINEA) FROM #DELTA_002#) AS FTN_NO_LINEA,
  'Tipo de registro' AS CAMPO,
  'Error en tipo de registro' AS VALIDACION,
  'N/A' AS FTC_LINEA,
  0 AS COD_ERROR,
  'Error falta encabezado: 01' AS DESC_ERROR,
  'N/A' AS VALOR_A_VALIDAR
FROM (
    SELECT DISTINCT SR_ID_ARCHIVO, FECHA_CARGA
    FROM #DELTA_002#
) D
WHERE NOT EXISTS (
    SELECT 1 FROM #DELTA_002# WHERE FTC_CONTROL = '01'
)


UNION ALL

-- Falta línea con código '09'
SELECT 
  SR_ID_ARCHIVO,
  FECHA_CARGA,
  NULL AS FTC_CONTROL,
  (SELECT MAX(FTN_NO_LINEA) FROM #DELTA_002#) AS FTN_NO_LINEA,
  'Tipo de registro' AS CAMPO,
  'Error en tipo de registro' AS VALIDACION,
  'N/A' AS FTC_LINEA,
  0 AS COD_ERROR,
  'Error falta sumario: 09' AS DESC_ERROR,
  'N/A' AS VALOR_A_VALIDAR
FROM (
    SELECT DISTINCT SR_ID_ARCHIVO, FECHA_CARGA
    FROM #DELTA_002#
) D
WHERE NOT EXISTS (
    SELECT 1 FROM #DELTA_002# WHERE FTC_CONTROL = '09'
)


  UNION ALL

--  Error de longitud 
  SELECT
    SR_ID_ARCHIVO,
    FECHA_CARGA,
    FTC_CONTROL,
    FTN_NO_LINEA,
    'Longitud de registro' AS CAMPO,
    'Error en longitud de registro' AS VALIDACION,
    FTC_LINEA,
    0 AS COD_ERROR,
    CASE
      WHEN FTN_NO_LINEA = 1 THEN 'Error de longitud en 01'
      WHEN FTN_NO_LINEA = (SELECT MAX(FTN_NO_LINEA) FROM #DELTA_002#) THEN 'Error de longitud en 09'
      ELSE 'Error de longitud en 02'
    END AS DESC_ERROR,
    'N/A' AS VALOR_A_VALIDAR
  FROM #DELTA_002#
  WHERE VAL_LENGTH_RECORD = 0
) ERRORES_TOTALES
ORDER BY FTN_NO_LINEA;


