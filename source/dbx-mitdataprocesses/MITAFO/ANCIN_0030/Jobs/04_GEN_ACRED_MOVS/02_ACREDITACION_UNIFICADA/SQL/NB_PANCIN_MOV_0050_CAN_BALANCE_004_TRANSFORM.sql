-- NB_PANCIN_MOV_0050_CAN_BALANCE_004_TRANSFORM.sql
-- Propósito: Aplicar transformaciones TF_130 desde tabla Delta del join
-- Tipo: Delta (Databricks) - SELECT con lógica de negocio desde tabla Delta
-- NOTA: Esta query implementa exactamente la lógica del transformer TF_130
-- ZONA HORARIA: México (America/Mexico_City)

-- Aplicar lógica de balance según tipo de movimiento desde tabla Delta
SELECT  ROW_NUMBER() OVER (ORDER BY FTC_FOLIO, FTC_FOLIO_REL) + MAXIMO1 AS FTN_ID_BAL_MOV,
        FTC_FOLIO,
        FTC_FOLIO_REL,
        -- Lógica de balance para Tipo 180 (Disponible/Aportaciones)
        CASE 
            WHEN FCN_ID_TIPO_MOV = 180 THEN FTF_MONTO_PESOS
            ELSE 0 
        END AS FTN_DISP_PESOS,
        CASE 
            WHEN FCN_ID_TIPO_MOV = 180 THEN FTF_MONTO_ACCIONES
            ELSE 0 
        END AS FTN_DISP_ACCIONES,
        -- Lógica de balance para Tipo 181 (Pendiente/Retiros)
        CASE 
            WHEN FCN_ID_TIPO_MOV = 181 THEN FTF_MONTO_PESOS * (-1)
            ELSE 0 
        END AS FTN_PDTE_PESOS,
        CASE 
            WHEN FCN_ID_TIPO_MOV = 181 THEN FTF_MONTO_ACCIONES * (-1)
            ELSE 0 
        END AS FTN_PDTE_ACCIONES,
        -- Lógica de balance para Tipo 180 (Comprometido)
        CASE 
            WHEN FCN_ID_TIPO_MOV = 180 THEN FTF_MONTO_PESOS * (-1)
            ELSE 0 
        END AS FTN_COMP_PESOS,
        CASE 
            WHEN FCN_ID_TIPO_MOV = 180 THEN FTF_MONTO_ACCIONES * (-1)
            ELSE 0 
        END AS FTN_COMP_ACCIONES,
        -- Campos que siempre son 0
        0 AS FTN_DIA_PESOS,
        0 AS FTN_DIA_ACCIONES,
        FTC_NUM_CTA_INVDUAL,
        CAST(NULL AS DECIMAL(10,0)) AS FTN_ORIGEN_APORTACION,
        FCN_ID_TIPO_SUBCTA,
        FCN_ID_SIEFORE,
        FCN_ID_VALOR_ACCION,
        FTD_FEH_LIQUIDACION,
        FCN_ID_TIPO_MOV,
        FCN_ID_CONCEPTO_MOV,
        FCC_TABLA_NCI_MOV,
        FTN_DEDUCIBLE,
        FCN_ID_PLAZO,
        FROM_UTC_TIMESTAMP(current_timestamp(), 'America/Mexico_City') AS FCD_FEH_CRE,
        '#CX_CRE_USUARIO#' AS FCC_USU_CRE,
        FROM_UTC_TIMESTAMP(current_timestamp(), 'America/Mexico_City') AS FCD_FEH_ACT,
        '#CX_CRE_USUARIO#' AS FCC_USU_ACT
FROM    #TABLA_JOIN_COMPLETO# 