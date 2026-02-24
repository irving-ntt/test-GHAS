--Plantilla: LayoutAGRechazados
--TRANSFERENCIA POR ANUALIDAD GARANTIZADA
--Subproceso: 365
--Ejemplo máscara nombre: PTCFT_20250807_TRANSAG_DEV.txt
WITH RES_02 AS(
	SELECT
		COUNT(1) ID_02
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_Registro
	,CASE WHEN CAST(substr(FTC_LINEA, 3, 9) AS BIGINT) = RES_02.ID_02 then NULL else substr(FTC_LINEA,3,9) end Cantidad_registros_detalle
	--,CASE WHEN substr(FTC_LINEA,12,719) = REPEAT(' ', 719) then NULL else substr(FTC_LINEA,12,719) end Filler
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_02 ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'