-- ==========================================
-- CONSULTA ORACLE: DB_200_CRE_BALANCE_MOVS
-- ==========================================
-- Extraer máximos desde Oracle para balance de movimientos
-- Equivalente al stage DB_200_CRE_BALANCE_MOVS
-- ==========================================

SELECT 
    NVL((SELECT MAX(FTN_ID_BAL_MOV) FROM #CX_CRN_ESQUEMA#.#TL_CRN_BALANCE_MOVS#), 0) AS MAXIMO,
    NVL((SELECT MAX(FTN_ID_MOV) FROM #CX_CRE_ESQUEMA#.#TL_CRE_MOVIMIENTOS#), 0) AS MAXIMO2,
    1 AS LLAVE 
FROM DUAL 