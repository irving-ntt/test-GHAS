--Plantilla: LayoutTAAceptados
--TRANSFERENCIAS DE ACREDITADOS INFONAVIT
--Subproceso: 364
--Ejemplo máscara nombre: PTCFTA_20250807_001_TRADEV.txt
WITH RES_02 AS(
	SELECT
		COUNT(1) ID_02
		,SUM(TRY_CAST(substr(FTC_LINEA,475,15) AS BIGINT)) ID_04
		,SUM(TRY_CAST(substr(FTC_LINEA,490,15) AS BIGINT)) ID_05
		,SUM(TRY_CAST(substr(FTC_LINEA,550,15) AS BIGINT)) ID_07
		,SUM(TRY_CAST(substr(FTC_LINEA,565,15) AS BIGINT)) ID_08
		,SUM(TRY_CAST(substr(FTC_LINEA,660,15) AS BIGINT)) ID_09
		,SUM(TRY_CAST(substr(FTC_LINEA,675,15) AS BIGINT)) ID_10
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN CAST(substr(FTC_LINEA, 3, 9) AS BIGINT) = RES_02.ID_02 then NULL else substr(FTC_LINEA,3,9) end Cantidad_registros_detalle
	--,CASE WHEN substr(FTC_LINEA,12,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,12,30) end Filler_01
	,CASE WHEN try_cast(substr(FTC_LINEA,42,18) as BIGINT) = RES_02.ID_04 then NULL else substr(FTC_LINEA,42,18) end Sumatoria_AIVS_vivienda_97
	,CASE WHEN try_cast(substr(FTC_LINEA,60,15) as BIGINT) = RES_02.ID_05 then NULL else substr(FTC_LINEA,60,15) end Sumatoria_saldo_vivienda_97
	--,CASE WHEN substr(FTC_LINEA,75,45) = REPEAT(' ', 45) then NULL else substr(FTC_LINEA,75,45) end Filler_02
	,CASE WHEN try_cast(substr(FTC_LINEA,120,18) as BIGINT) = RES_02.ID_07 then NULL else substr(FTC_LINEA,120,18) end Sumatoria_AIVS_vivienda_92
	,CASE WHEN try_cast(substr(FTC_LINEA,138,15) as BIGINT) = RES_02.ID_08 then NULL else substr(FTC_LINEA,138,15) end Sumatoria_saldo_vivienda_92
	,CASE WHEN try_cast(substr(FTC_LINEA,153,15) as BIGINT) = RES_02.ID_09 then NULL else substr(FTC_LINEA,153,15) end Intereses_Vivienda_97
	,CASE WHEN try_cast(substr(FTC_LINEA,168,15) as BIGINT) = RES_02.ID_10 then NULL else substr(FTC_LINEA,168,15) end Intereses_Vivienda_92
	--,CASE WHEN substr(FTC_LINEA,183,548) = REPEAT(' ', 548) then NULL else substr(FTC_LINEA,183,548) end Filler_03
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_02 ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'