--Plantilla: LayoutAGAceptados
--TRANSFERENCIA POR ANUALIDAD GARANTIZADA
--Subproceso: 365
--Ejemplo máscara nombre: PTCFT_20250807_TRANSAG_DEV.txt
WITH RES_02 AS(
	SELECT
		COUNT(1) ID_02
		,SUM(TRY_CAST(substr(FTC_LINEA,475,15) AS BIGINT)) ID_04
		,SUM(TRY_CAST(substr(FTC_LINEA,550,15) AS BIGINT)) ID_05
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN CAST(substr(FTC_LINEA, 3, 9) AS BIGINT) = RES_02.ID_02 then NULL else substr(FTC_LINEA,3,9) end Cantidad_registros_detalle
	--,CASE WHEN substr(FTC_LINEA,12,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,12,30) end Filler_01
	,CASE WHEN CAST(substr(FTC_LINEA, 42, 18) AS BIGINT) = RES_02.ID_04 then NULL else substr(FTC_LINEA,42,18) end Suma_Aplicaciones_Intereses_Viv_92
	,CASE WHEN CAST(substr(FTC_LINEA, 60, 18) AS BIGINT) = RES_02.ID_05 then NULL else substr(FTC_LINEA,60,15) end Suma_Aplicaciones_Intereses_Viv_97
	--,CASE WHEN substr(FTC_LINEA,72,659) = REPEAT(' ', 659) then NULL else substr(FTC_LINEA,72,659) end Filler_02
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_02 ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'