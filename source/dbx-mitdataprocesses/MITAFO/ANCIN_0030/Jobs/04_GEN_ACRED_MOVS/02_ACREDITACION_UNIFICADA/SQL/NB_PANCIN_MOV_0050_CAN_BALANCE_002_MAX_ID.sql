-- NB_PANCIN_MOV_0050_CAN_BALANCE_002_MAX_ID.sql
-- Propósito: Obtener el máximo ID de balance existente en la tabla TTAFOGRAL_BALANCE_MOVS
-- Tipo: OCI (Oracle) - SELECT con NVL

SELECT  NVL((SELECT MAX(FTN_ID_BAL_MOV) 
             FROM #CX_CRN_ESQUEMA#.#TL_CRN_BALANCE_MOVS#), 0) AS MAXIMO1,
        1 AS LLAVE
FROM    dual 