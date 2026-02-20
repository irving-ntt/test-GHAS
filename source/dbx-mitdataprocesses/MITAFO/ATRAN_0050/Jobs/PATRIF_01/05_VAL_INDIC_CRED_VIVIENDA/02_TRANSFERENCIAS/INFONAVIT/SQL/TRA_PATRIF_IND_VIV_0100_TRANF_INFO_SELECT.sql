    SELECT 
    M.FTN_CONTA_SERV,
    M.FTC_FOLIO_BITACORA,
    M.FTN_NUM_CTA_AFORE,
    -- Lógica para FTD_FEH_ACTUA_ESTATUS
    CASE 
        WHEN (
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 2 THEN 538 --p_RECH_AT_TA
                WHEN TRIM(FCC_VALOR_IND) = 3 THEN 539 --P_RECH_UG_AG
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 1 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678 --P_RECH_GEN
            END) IS NULL OR 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 1 THEN 676 --P_RECH_TA_UG
                WHEN TRIM(FCC_VALOR_IND) = 3 THEN 585 --P_RECH_TA_AG
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 2 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678 --P_RECH_GEN
            END) IS NULL OR 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 1 THEN 631
                WHEN TRIM(FCC_VALOR_IND) = 2 THEN 630
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 3 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678 --P_RECH_GEN
            END) IS NULL
        ) 
        THEN current_date --FTD_FEH_ACTUA_ESTATUS 
        ELSE current_date -- Cambiado a SYSDATE
    END AS FTD_FEH_ACTUA_ESTATUS,
    -- Lógica para FTD_FEH_ACT
    CASE 
        WHEN ( 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 2 THEN 538
                WHEN TRIM(FCC_VALOR_IND) = 3 THEN 539
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 1 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678
            END) IS NULL OR 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 1 THEN 676
                WHEN TRIM(FCC_VALOR_IND) = 3 THEN 585
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 2 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678
            END) IS NULL OR 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 1 THEN 631
                WHEN TRIM(FCC_VALOR_IND) = 2 THEN 630
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 3 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678
            END) IS NULL 
        ) THEN current_date -- M.FTD_FEH_ACT 
        ELSE current_date -- Cambiado a SYSDATE
    END AS FTD_FEH_ACT,
    -- Lógica para FTD_USU_ACT
    CASE 
        WHEN ( 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 2 THEN 538
                WHEN TRIM(FCC_VALOR_IND) = 3 THEN 539
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 1 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678
            END) IS NULL OR 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 1 THEN 676
                WHEN TRIM(FCC_VALOR_IND) = 3 THEN 585
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 2 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678
            END) IS NULL OR 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 1 THEN 631
                WHEN TRIM(FCC_VALOR_IND) = 2 THEN 630
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 3 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE 678
            END) IS NULL 
        ) THEN 'DATABRICKS' --M.FCC_USU_ACT 
        ELSE 'DATABRICKS' 
    END AS FCC_USU_ACT,
    -- Lógica para FTN_MOTIVO_RECHAZO
    CASE 
        WHEN FTN_ID_SUBP = 368 THEN 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 2 THEN NULL --538
                WHEN TRIM(FCC_VALOR_IND) = 3 THEN NULL --539
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 1 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE NULL --678
            END)
        WHEN FTN_ID_SUBP = 364 THEN 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 1 THEN NULL --676
                WHEN TRIM(FCC_VALOR_IND) = 3 THEN NULL --585
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 2 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE NULL --678
            END)
        WHEN FTN_ID_SUBP = 365 THEN 
            (CASE 
                WHEN TRIM(FCC_VALOR_IND) = 1 THEN NULL --631
                WHEN TRIM(FCC_VALOR_IND) = 2 THEN NULL --630
                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                WHEN FCC_VALOR_IND = 3 OR FCC_VALOR_IND = 4 THEN NULL
                ELSE NULL --678
            END)
        ELSE NULL 
    END AS FTN_MOTIVO_RECHAZO,
    -- Lógica para FTC_ESTATUS_REGISTRO
    CASE 
        WHEN FTN_ID_SUBP = 368 THEN 
            CASE WHEN(
                (CASE 
                    WHEN TRIM(FCC_VALOR_IND) = 2 THEN 538
                    WHEN TRIM(FCC_VALOR_IND) = 3 THEN 539
                    WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
                    WHEN FCC_VALOR_IND = 1 OR FCC_VALOR_IND = 4 THEN NULL
                    ELSE 678
                END)
            ) IS NULL THEN 1 ELSE 1 END --ANTES ELSE 0
        WHEN FTN_ID_SUBP = 364 THEN 
            CASE WHEN (
	        	(CASE 
	                WHEN TRIM(FCC_VALOR_IND) = 1 THEN 676
	                WHEN TRIM(FCC_VALOR_IND) = 3 THEN 585
	                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
	                WHEN FCC_VALOR_IND = 2 OR FCC_VALOR_IND = 4 THEN NULL
	                ELSE 678
	            END)
	        ) IS NULL THEN 1 ELSE 1 END --ANTES ELSE 0
        WHEN FTN_ID_SUBP = 365 THEN 
            CASE WHEN(
	        	(CASE 
	                WHEN TRIM(FCC_VALOR_IND) = 1 THEN 631
	                WHEN TRIM(FCC_VALOR_IND) = 2 THEN 630
	                WHEN FFN_ID_CONFIG_INDI IS NULL AND FCC_VALOR_IND IS NULL THEN NULL
	                WHEN FCC_VALOR_IND = 3 OR FCC_VALOR_IND = 4 THEN NULL
	                ELSE 678
	            END)
	        ) IS NULL THEN 1 ELSE 1 END --ANTES ELSE 0
        ELSE 1 --NULL 
    END AS FTC_ESTATUS_REGISTRO,
    364 as FTN_ID_SUBP 
FROM 
    PROCESOS.TTCRXGRAL_TRANS_INFONA M 
LEFT JOIN 
    CIERREN.TTAFOGRAL_IND_CTA_INDV C ON 
    M.FTN_NUM_CTA_AFORE = C.FTN_NUM_CTA_INVDUAL 
    AND M.FTC_FOLIO_BITACORA = '#sr_folio#'
    AND M.FTC_ESTATUS_REGISTRO = '#ind_vigencia#' 
    AND C.FFN_ID_CONFIG_INDI ='#config_indi#'
    AND C.FTC_VIGENCIA = '#ind_vigencia#' 
WHERE 
	M.FTC_FOLIO_BITACORA = '#sr_folio#' 
    AND M.FTC_ESTATUS_REGISTRO = '#ind_vigencia#'



