--Plantilla: Validacion_Layout_de_respuesta_Transferencias_FOVISSSTE_v1.1 1
--TRANSFERENCIAS DE ACREDITADOS FOVISSSTE
--Subproceso: 363
--Ejemplo máscara nombre: PREFT.DP.A01534.NOTDEV.GDG
WITH RES_02 AS(
	SELECT
		COUNT(1) ID_02
		,SUM(TRY_CAST(substr(FTC_LINEA,66,15) AS BIGINT)) ID_03
		,SUM(TRY_CAST(substr(FTC_LINEA,101,15) AS BIGINT)) ID_04
		,SUM(TRY_CAST(substr(FTC_LINEA,118,15) AS BIGINT)) ID_05
		,SUM(TRY_CAST(substr(FTC_LINEA,153,15) AS BIGINT)) ID_06
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN TRY_CAST(substr(FTC_LINEA, 3, 9) AS BIGINT) = RES_02.ID_02 then NULL else substr(FTC_LINEA,3,9) end Cantidad_registros_detalle
	,CASE WHEN TRY_CAST(substr(FTC_LINEA, 12, 18) AS BIGINT) = RES_02.ID_03 then NULL else substr(FTC_LINEA,12,18) end Aplicacion_Intereses_Viv_FOVISSSTE_92
	,CASE WHEN TRY_CAST(substr(FTC_LINEA, 30, 15) AS BIGINT) = RES_02.ID_04 then NULL else substr(FTC_LINEA,30,15) end Importe_Fondo_viv_FOVISSSTE_92
	,CASE WHEN TRY_CAST(substr(FTC_LINEA, 45, 18) AS BIGINT) = RES_02.ID_05 then NULL else substr(FTC_LINEA,45,18) end Aplicacion_Intereses_Viv_FOVISSSTE_2008
	,CASE WHEN TRY_CAST(substr(FTC_LINEA, 63, 15) AS BIGINT) = RES_02.ID_06 then NULL else substr(FTC_LINEA,63,15) end Importe_Fondo_viv_FOVISSSTE_2008
	--,Codigo_Resultado_de_la_Operacion
	--,Diagnostico_del_Proceso
	--,Filler
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_02 ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'