SELECT 
CASE WHEN (
				CASE 
				    WHEN (1 = 1 AND '02' = '02') OR
				         (2 = 2 AND '01' = '01') OR
				         (3 = 3 AND '04' = '04') 
				    THEN NULL
				    WHEN 4 = 4 OR
				         NULL IS NULL OR
				         ('03' NOT IN ('01', '02', '04'))
				    THEN 677
				    ELSE 677
				END
				) = NULL
		THEN (FROM_TZ(CAST(SYSTIMESTAMP AS TIMESTAMP), 'UTC') AT TIME ZONE 'America/Mexico_City')
		ELSE  
			(FROM_TZ(CAST(SYSTIMESTAMP AS TIMESTAMP), 'UTC') AT TIME ZONE 'America/Mexico_City')
		END
		AS FTD_FEH_ACT
FROM PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO