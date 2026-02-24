-- Verificar si existe archivo de control para el folio especificado
-- Equivalente al comando: if [ -f "3281_{SR_FOLIO}.txt" ] ; then echo "1" ; else echo "2" ; fi

SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN '1'  -- Archivo existe
        ELSE '2'                     -- Archivo no existe
    END AS CONTROL_VALUE,
    COUNT(*) AS TOTAL_REGISTROS,
    MAX(FECHA_CREACION) AS ULTIMA_FECHA_CREACION,
    MAX(ESTADO) AS ESTADO_ACTUAL
FROM #CATALOG#.#SCHEMA#.CONTROL_ARCHIVOS_PANCIN
WHERE SR_FOLIO = '#SR_FOLIO#'
  AND ESTADO = 'ACTIVO'; 