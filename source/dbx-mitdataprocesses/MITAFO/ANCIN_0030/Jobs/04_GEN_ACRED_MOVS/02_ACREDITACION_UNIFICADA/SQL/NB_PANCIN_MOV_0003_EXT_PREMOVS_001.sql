-- ==========================================
-- QUERY: NB_PANCIN_MOV_0003_EXT_PREMOVS_001.sql
-- DESCRIPCIÓN: Consulta a OCI para extraer información de la marca de la tabla 
--              THAFOGRAL_MATRIZ_CONVIVENCIA y resguardar la información de las 
--              cuentas marcadas de los movimientos que se generarán
-- AUTOR: Sistema
-- FECHA: 2024-01-15
-- VERSIÓN: 1.0.0
-- ==========================================
-- PARÁMETROS:
--   @CX_CRE_ESQUEMA: Esquema de OCI para staging
--   @CX_CRN_ESQUEMA: Esquema de OCI para CIERREN
--   @TL_CRE_PRE_MOVIMIENTOS: Tabla de movimientos previos
--   @TL_CRN_MATRIZ_CONVIVENCIA: Tabla de matriz de convivencia
--   @SR_FOLIO: Folio del proceso
-- ==========================================
-- TIPO: OCI (Oracle)
-- USO: db.read_data()
-- ==========================================

WITH PRE AS (
    SELECT DISTINCT FTN_NUM_CTA_INVDUAL
    FROM #CX_CRE_ESQUEMA#.#TL_CRE_PRE_MOVIMIENTOS# PRE
    WHERE PRE.FTC_FOLIO = '#SR_FOLIO#'
      AND PRE.FTN_PRE_MOV_GENERADO IN (0,1)
    
    UNION
    
    SELECT DISTINCT FTN_NUM_CTA_INVDUAL
    FROM #CX_CRE_ESQUEMA#.#TL_CRE_PRE_MOVIMIENTOS# PRE
    WHERE PRE.FTC_FOLIO_REL = '#SR_FOLIO#'
      AND PRE.FTN_PRE_MOV_GENERADO IN (0,1)
)
SELECT 
    FTN_ID_MARCA,
    a.FTC_FOLIO,
    a.FTN_NUM_CTA_INVDUAL,
    FRN_ID_MOV_SUBCTA,
    DECODE(FTB_ESTATUS_MARCA, NULL, 0, FTB_ESTATUS_MARCA) AS FTB_ESTATUS_MARCA
FROM #CX_CRN_ESQUEMA#.#TL_CRN_MATRIZ_CONVIVENCIA# a, 
     (SELECT FTN_NUM_CTA_INVDUAL FROM PRE) b
WHERE a.FTN_NUM_CTA_INVDUAL = b.FTN_NUM_CTA_INVDUAL 