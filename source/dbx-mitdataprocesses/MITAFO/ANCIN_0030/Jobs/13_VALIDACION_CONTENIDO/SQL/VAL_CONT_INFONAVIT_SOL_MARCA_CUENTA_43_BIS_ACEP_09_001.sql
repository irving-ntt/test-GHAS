--Plantilla: Layout de Notificación de Saldo (Aceptados)
--SOLICITUD DE MARCA DE CUENTAS POR 43 BIS
--Subproceso: 354
--Ejemplo máscara nombre: PTCFTA_AAAAMMDD_CCC_ACEPMARCA43.TXT
WITH RES_02 AS(
	SELECT
		COUNT(1) ID_02
		,SUM(TRY_CAST(substr(FTC_LINEA,475,15) AS BIGINT)) ID_03
		,SUM(TRY_CAST(substr(FTC_LINEA,490,15) AS BIGINT)) ID_04
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN CAST(substr(FTC_LINEA, 3, 9) AS BIGINT) = RES_02.ID_02 then NULL else substr(FTC_LINEA,3,9) || '!=' || CAST(RES_02.ID_02 as string) end Cantidad_registros_detalle
	--,Filler
	,CASE WHEN CAST(substr(FTC_LINEA, 42, 18) AS BIGINT) = RES_02.ID_03 then NULL else substr(FTC_LINEA,42,18) || '!=' || CAST(RES_02.ID_03 as string) end Suma_numero_Aplicaciones_viv_97
	,CASE WHEN CAST(substr(FTC_LINEA, 61, 15) AS BIGINT) = RES_02.ID_04 then NULL else substr(FTC_LINEA,61,15) || '!=' || CAST(RES_02.ID_04 as string) end Suma_saldo_Viv_97
	--,CASE WHEN substr(FTC_LINEA,72,659) = REPEAT(' ', 659) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,72,659))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,72,659) end Filler
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_02 ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'